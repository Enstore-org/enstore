
#include <stdio.h>
#include <ftt_private.h>

int ftt_trans_open[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_ENOENT,
	/*    5	EIO	*/	FTT_ENOTAPE,
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
};

int ftt_trans_in_Linux[MAX_TRANS_ERRNO] = {
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
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
	/*   50 default */	FTT_EIO,
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
int *ftt_trans_table_Linux[] = {
    /* none...but just to be safe0 */ ftt_trans_in,
    /* FTT_OPN_READ		     1 */ ftt_trans_in_Linux,
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
 	"35G 62500bpi 224trk",
	0
};

char *AIT_density_trans[MAX_TRANS_DENSITY] = {
	"unknown",
	"AIT-1",
	"AIT-2",
	0
};


/*
** commands to popen to read device id-s
** and echo the device type as the result
** note the %d is filled by a printf first
*/
static char LINUXfind[] =
    "IFS=\" \t\n\";\
     PATH=\"/bin:/usr/bin:/sbin:/usr/sbin\";\
     count=0; export count ;\
     cat /proc/scsi/scsi | while read a b c d e f; do \
        if [ \"$c\" = \"Model:\" ]; then mod=$d; fi ; \
        if [ \"$b\" = Sequential-Access ] ; then \
		if [ \"$count\" = \"%d\" ] ; then \
		    echo $mod; exit; \
		fi; \
		count=`expr $count + 1`; \
	fi; \
       done";

static char LINUXrmtfind[] =
    "IFS=\" \t\n\";\
     PATH=\"/bin:/usr/bin:/sbin:/usr/sbin\";\
     cat /proc/scsi/scsi | while read a b c d e f g h; do \
        if [ \"$b\" = \"scsi%d\" -a \"$f\" = \"%02d\" ] ; then \
	    read a b c d e f g h ;\
	    echo $d; exit; \
	fi; \
       done";

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
       sed -e 's/.*: *//' -e 's/8mm *(\\(.*\\)) *cartridge */\\1/' -e 's/^8.0.$/Exb-&/'";

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

static char SunOSfind_dev[] =
   "#d\n\
    PATH=\"/bin:/usr/bin:/sbin:/usr/sbin\";\
    drive=%d\n\
    adr=\"`ls -l /dev/rmt/${drive} | awk '{print substr($NF,index($NF,\\\"st@\\\"),length($NF)-index($NF,\\\"st@\\\"))}'`\" \n\
    ctr=\"`ls -l /dev/rmt/${drive} | awk '{if(index($NF,\\\"/scsi@\\\")==0) {print substr($NF,index($NF,\\\"device\\\")+7,index($NF,\\\"/st@\\\")-index($NF,\\\"device\\\")-7)} else{print substr($NF,index($NF,\\\"device\\\")+7,index($NF,\\\"/scsi@\\\")-index($NF,\\\"device\\\")-6)}}'`\" \n\
    id=\"`dmesg | grep $ctr | grep $adr | awk '{print $1; exit}'`\" \n\
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
**    char *baseconv_out;          basename parser printf string 
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
#define EXB_MAX_BLKSIZE  245760
#define IRIX_MAX_BLKSIZE 131072
#define SUN_MAX_BLKSIZE   EXB_MAX_BLKSIZE
#define WIN_MAX_BLKSIZE  120000
#ifdef NICE_WORLD
#define LINUX_MAX_BLKSIZE 65536
#else
#define LINUX_MAX_BLKSIZE 32768
#endif


ftt_dev_entry devtable[] = {
    {"Linux", "SDX-3", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, AIT_density_trans,
       "rmt/tps%dd%d", "rmt/tps%dd%dn", 2, LINUXrmtfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "rmt/tps%dd%dn",  0,  0, 0x00,  0,  0,      0,  1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "sc/sc%dd%d",    -1,  0, -1,    1,  0,  0,      1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/tps%dd%dn",  0,  1, 0x30,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",  0,  0, 0x30,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%d",   0,  1, 0x30,  0,  0,FTT_RWOC, 1, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%d",   0,  0, 0x30,  0,  0,FTT_RWOC, 0, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "SDX-5", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, AIT_density_trans,
       "rmt/tps%dd%d", "rmt/tps%dd%dn", 2, LINUXrmtfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "rmt/tps%dd%dn",  1,  0, 0x00,  0,  0,      0,  1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "sc/sc%dd%d",    -1,  0, -1,    1,  0,  0,      1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/tps%dd%dn",  0,  0, 0x30,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%d",   1,  0, 0x31,  0,  0,FTT_RWOC, 1, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%d",   0,  0, 0x30,  0,  0,FTT_RWOC, 0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",  1,  1, 0x31,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",  0,  1, 0x30,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%d",   1,  1, 0x31,  0,  0,FTT_RWOC, 0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%d",   0,  1, 0x30,  0,  0,FTT_RWOC, 0, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "DLT7", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER,
	FTT_OP_GET_STATUS, ftt_trans_table_Linux, DLT_density_trans,
       "rmt/tps%dd%d", "rmt/tps%dd%dn", 2, LINUXrmtfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "rmt/tps%dd%dn",    6,  0, 0x84, 0,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "sc/sc%dd%d",     -1,  0, -1,  1,  0,       0,     1, LINUX_MAX_BLKSIZE},
    /* Other Densities */
       { "rmt/tps%dd%dn",    6,  1, 0x85, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    4,  0, 0x80, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    4,  1, 0x81, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    5,  0, 0x1A, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    5,  0, 0x82, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    5,  1, 0x83, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    4,  0, 0x19, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    3,  0, 0x18, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    2,  0, 0x17, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    1,  0, 0x16, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    0,  0, 0x0A, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/tps%dd%d",      0,  0,  0,  0,  0, FTT_RWOC,  1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "DLT4", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER,
	FTT_OP_GET_STATUS, ftt_trans_table_Linux, DLT_density_trans,
       "rmt/tps%dd%d", "rmt/tps%dd%dn", 2, LINUXrmtfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "rmt/tps%dd%dn",    5,  0, 0x1A, 0,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "sc/sc%dd%d",     -1,  0, -1,  1,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Other Densities */
       { "rmt/tps%dd%dn",    4,  0, 0x80, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    4,  1, 0x81, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    5,  0, 0x82, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    5,  1, 0x83, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    4,  0, 0x19, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    3,  0, 0x18, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    2,  0, 0x17, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    1,  0, 0x16, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    0,  0, 0x0A, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/tps%dd%d",      0,  0,  0,  0,  0, FTT_RWOC,  1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "DLT2", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , 
	FTT_OP_GET_STATUS, ftt_trans_table_Linux, DLT_density_trans,
       "rmt/tps%dd%d", "rmt/tps%dd%dn", 2, LINUXrmtfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "rmt/tps%dd%dn",    4,  0, 0x80, 0,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "sc/sc%dd%d",     -1,  0, -1,  1,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Other Densities */
       { "rmt/tps%dd%dn",    4,  1, 0x81, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    5,  0, 0x82, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    5,  1, 0x83, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    0,  0, 0x0A, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    4,  0, 0x19, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    3,  0, 0x18, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    2,  0, 0x17, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",    1,  0, 0x16, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/tps%dd%d",      0,  0,  0,  0,  0, FTT_RWOC,  1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "EXB-82", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, Exabyte_density_trans,
       "rmt/tps%dd%d", "rmt/tps%dd%dn", 2, LINUXrmtfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "rmt/tps%dd%dn",     0,  0,  0,  0,  0,      0,    1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "sc/sc%dd%d",     -1,  0, -1,  1,  0,      0,    1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/tps%dd%d",      0,  0,  0,  0,  0, FTT_RWOC,  1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "EXB-8505", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, Exabyte_density_trans,
       "rmt/tps%dd%d", "rmt/tps%dd%dn", 2, LINUXrmtfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "rmt/tps%dd%dn", 	  1,  0, 0x15,  0,  0,      0,  1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "sc/sc%dd%d",     -1,  0, -1,    1,  0,  0,      1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/tps%dd%dn", 	  1,  1, 0x8c,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn", 	  1,  0, 0x15,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn", 	  0,  1, 0x90,  0,  0,FTT_RDNW, 0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn", 	  0,  0, 0x14,  0,  0,FTT_RDNW, 0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%d",      1,  0, 0x15,  0,  0,FTT_RWOC, 1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "EXB-85", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, Exabyte_density_trans,
       "rmt/tps%dd%d", "rmt/tps%dd%dn", 2, LINUXrmtfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "rmt/tps%dd%dn", 	  1,  0, 0x15,  0,  0,      0,  1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "sc/sc%dd%d",     -1,  0, -1,    1,  0,  0,      1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/tps%dd%dn", 	  1,  0, 0x15,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn", 	  0,  0, 0x14,  0,  0,FTT_RDNW, 0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%d",      1,  0, 0x15,  0,  0,FTT_RWOC, 1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "EXB-89", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, Exabyte_density_trans,
       "rmt/tps%dd%d", "rmt/tps%dd%dn", 2, LINUXrmtfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "rmt/tps%dd%dn",  2,  1, 0x27,  0,  0,      0,  1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "sc/sc%dd%d",     -1,  0, -1,    1,  0,  0,      1, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn",  2,  0, 0x27,  0,  0,      0,  1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/tps%dd%dn", 	  1,  1, 0x8c,  0,  0,FTT_RDNW,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn", 	  1,  0, 0x15,  0,  0,FTT_RDNW,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn", 	  1,  0, 0x15,  0,  0,FTT_RDNW,  0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn", 	  0,  0, 0x14,  0,  0,FTT_RDNW, 0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%dn", 	  0,  1, 0x90,  0,  0,FTT_RDNW, 0, LINUX_MAX_BLKSIZE},
       { "rmt/tps%dd%d",      1,  0, 0x15,  0,  0,FTT_RWOC, 1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "DLT7", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, DLT_density_trans,
       "dev/%*[ns]t%d", "dev/nst%d", 1, LINUXfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "dev/nst%d",    6,  0, 0x84, 0,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "dev/sg%d",     -1,  0, -1,  1,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Other Densities */
       { "dev/nst%d",    6,  1, 0x85, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    4,  0, 0x80, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    4,  1, 0x81, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    5,  0, 0x1A, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    5,  0, 0x82, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    5,  1, 0x83, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    4,  0, 0x19, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    3,  0, 0x18, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    2,  0, 0x17, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    1,  0, 0x16, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    0,  0, 0x0A, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "dev/st%d",      0,  0,  0,  0,  0, FTT_RWOC,  1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "DLT4", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, DLT_density_trans,
       "dev/%*[ns]t%d", "dev/nst%d", 1, LINUXfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "dev/nst%d",    5,  0, 0x1A, 0,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "dev/sg%d",     -1,  0, -1,  1,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Other Densities */
       { "dev/nst%d",    4,  0, 0x80, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    4,  1, 0x81, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    5,  0, 0x82, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    5,  1, 0x83, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    4,  0, 0x19, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    3,  0, 0x18, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    2,  0, 0x17, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    1,  0, 0x16, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    0,  0, 0x0A, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "dev/st%d",      0,  0,  0,  0,  0, FTT_RWOC,  1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "DLT2", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, DLT_density_trans,
       "dev/%*[ns]t%d", "dev/nst%d", 1, LINUXfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "dev/nst%d",    4,  0, 0x80, 0,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "dev/sg%d",     -1,  0, -1,  1,  0,       0,   1, LINUX_MAX_BLKSIZE},
    /* Other Densities */
       { "dev/nst%d",    4,  1, 0x81, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    5,  0, 0x82, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    5,  1, 0x83, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    0,  0, 0x0A, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    4,  0, 0x19, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    3,  0, 0x18, 0,  0,       0,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    2,  0, 0x17, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d",    1,  0, 0x16, 0,  0,FTT_RDNW,   0, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "dev/st%d",      0,  0,  0,  0,  0, FTT_RWOC,  1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "EXB-82", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, Exabyte_density_trans,
       "dev/%*[ns]t%d", "dev/nst%d", 1, LINUXfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "dev/nst%d",     0,  0,  0,  0,  0,      0,    1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "dev/sg%d",     -1,  0, -1,  1,  0,      0,    1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "dev/nst%d", 	  0,  1, 0x90,  0,  0,FTT_RDNW, 0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  1,  0, 0x15,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  1,  1, 0x8c,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  0,  0, 0x14,  0, 0,FTT_RDNW,  0, LINUX_MAX_BLKSIZE},
       { "dev/st%d",      0,  0,  0,  0,  0, FTT_RWOC,  1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "EXB-8505", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, Exabyte_density_trans,
       "dev/%*[ns]t%d", "dev/nst%d", 1, LINUXfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "dev/nst%d", 	  1,  0, 0x15,  0,  0,      0,  1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "dev/sg%d",     -1,  0, -1,    1,  0,  0,      1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "dev/nst%d", 	  0,  1, 0x90,  0,  0,FTT_RDNW, 0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  1,  0, 0x15,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  1,  1, 0x8c,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  0,  0, 0x14,  0,  0,FTT_RDNW,  0, LINUX_MAX_BLKSIZE},
       { "dev/st%d",      1,  0, 0x15,  0,  0,FTT_RWOC, 1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "EXB-85", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, Exabyte_density_trans,
       "dev/%*[ns]t%d", "dev/nst%d", 1, LINUXfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "dev/nst%d", 	  1,  0, 0x15,  0,  0,      0,  1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "dev/sg%d",     -1,  0, -1,    1,  0,  0,      1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "dev/nst%d", 	  1,  0, 0x15,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  0,  0, 0x14,  0,  0,FTT_RDNW,  0, LINUX_MAX_BLKSIZE},
       { "dev/st%d",      1,  0, 0x15,  0,  0,FTT_RWOC, 1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "EXB-89", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, Exabyte_density_trans,
       "dev/%*[ns]t%d", "dev/nst%d", 1, LINUXfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "dev/nst%d", 	  2,  1, 0x27,  0,  0,      0,  1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "dev/sg%d",     -1,  0, -1,    1,  0,  0,      1, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  2,  0, 0x27,  0,  0,      0,  0, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "dev/nst%d", 	  1,  0, 0x15,  0,  0,FTT_RDNW,  0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  0,  1, 0x90,  0,  0,FTT_RDNW, 0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  1,  0, 0x15,  0,  0,FTT_RDNW,  0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  1,  1, 0x8c,  0,  0,FTT_RDNW,  0, LINUX_MAX_BLKSIZE},
       { "dev/nst%d", 	  0,  0, 0x14,  0,  0,FTT_RDNW,  0, LINUX_MAX_BLKSIZE},
       { "dev/st%d",      1,  0, 0x15,  0,  0,FTT_RWOC, 1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "", "SCSI",  FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, Generic_density_trans,
       "rmt/tps%dd%d", "rmt/tps%dd%dn", 2, LINUXrmtfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "rmt/tps%dd%dn",     0,  0,  0,  0,  0,  0,        1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "sc/sc%dd%d",     -1,  0, -1,  1,  0,  0,        1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/tps%dd%d",      0,  0,  0,  0,  0, FTT_RWOC,  1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},
    {"Linux", "", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_MODE_AFTER|FTT_FLAG_BSIZE_AFTER , FTT_OP_GET_STATUS, ftt_trans_table_Linux, Generic_density_trans,
       "dev/%*[ns]t%d", "dev/nst%d", 1, LINUXfind, {
    /*   string          den mod hwd  pas fxd rewind   1st */
    /*   ======          === === ===  === === ======   === */
    /* Default */
       { "dev/nst%d",     0,  0,  0,  0,  0,  0,        1, LINUX_MAX_BLKSIZE},
    /* Default, passthru  */
       { "dev/sg%d",     -1,  0, -1,  1,  0,  0,        1, LINUX_MAX_BLKSIZE},
    /* Descriptive */
       { "dev/st%d",      0,  0,  0,  0,  0, FTT_RWOC,  1, LINUX_MAX_BLKSIZE},
       { 0 },
    }},

    {"SunOS+5", "EXB-8900", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_VERIFY_EOFS, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
       "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {
    /*   string          den mod hwd   pas fxd rewind            1st */
    /*   ======          === === ===   === === ======            === */
    /* Default, passthru  */
       { "rmt/%dhbn", 	  2,  1, 0x27,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhbn", 	  2,  1, 0x27,  1,  0,                 0, 0, SUN_MAX_BLKSIZE},
    /* Usable */
       { "rmt/%dhbn", 	  2,  0, 0x27,  0,  0,                 0, 0, SUN_MAX_BLKSIZE},
    /* Densitites */
       { "rmt/%dub", 	  0,  1, 0x90,  0,  0,FTT_RDNW| FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhb", 	  1,  0, 0x15,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmb", 	  1,  1, 0x8c,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dlb", 	  0,  0, 0x14,  0,  0,FTT_RDNW| FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%db", 	  1,  0, 0x15,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/%dubn", 	  0,  1, 0x90,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
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
    {"SunOS+5", "EXB-8200", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_VERIFY_EOFS, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
       "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, pass-thru */
       { "rmt/%dubn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dubn", 	  0,  0,  0,    1,  0,                 0, 0, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/%dhbn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmbn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dlbn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dub", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhb", 	  0,  0,  0,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmb", 	  0,  0,  0,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dlb", 	  0,  0,  0,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dun", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhn", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmn", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dln", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%du", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dh", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dm", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dl", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dc", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcb", 	  1,  1,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcbn", 	  1,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcn", 	  1,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},
    {"SunOS+5", "EXB-85", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_VERIFY_EOFS, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
       "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru  */
       { "rmt/%dmbn", 	  1,  0, 0x15,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
    /* Usable Varirable */
       { "rmt/%dlbn", 	  0,  0, 0x14,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
    /* Densitites */
       { "rmt/%dub", 	  0,  1, 0x90,  0,  0,FTT_RDNW| FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhb", 	  1,  0, 0x15,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmb", 	  1,  1, 0x8c,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dlb", 	  0,  0, 0x14,  0,  0,FTT_RDNW| FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%db", 	  1,  0, 0x15,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/%dubn", 	  0,  1, 0x90,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmbn", 	  1,  0, 0x15,  1,  0, FTT_RDNW|       0, 0, SUN_MAX_BLKSIZE},
       { "rmt/%dhbn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
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
    {"SunOS+5", "DLT", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_VERIFY_EOFS, FTT_OP_GET_STATUS, ftt_trans_table, DLT_density_trans,
       "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {
    /*   string          den mod hwd   pas fxd rewind            1st */
    /*   ======          === === ===   === === ======            === */
    /* Default, Passthru  */
       { "rmt/%dubn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "rmt/%dubn", 	  5,  1, 0x1A,  0,  0,                 0, 0, EXB_MAX_BLKSIZE},
       { "rmt/%dubn", 	  5,  0, 0x00,  1,  0,                 0, 0, EXB_MAX_BLKSIZE},
       { "rmt/%dhbn", 	  4,  0, 0x19,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "rmt/%dhbn", 	  4,  1, 0x19,  0,  0,                 0, 0, EXB_MAX_BLKSIZE},
       { "rmt/%dmbn", 	  3,  0, 0x18,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "rmt/%dlbn", 	  2,  0, 0x17,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Densitites */
       { "rmt/%d", 	  0,  0, 0x0A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  1,  0, 0x16,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  2,  0, 0x17,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  3,  0, 0x18,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  4,  0, 0x80,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  4,  1, 0x81,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  4,  0, 0x19,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  4,  1, 0x19,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  5,  0, 0x82,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  5,  1, 0x83,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  5,  1, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/%dcbn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "rmt/%dbn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "rmt/%dub", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "rmt/%dcb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "rmt/%dhb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "rmt/%dmb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "rmt/%dlb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "rmt/%db", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "rmt/%dun", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhn", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmn", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dln", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%du", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dh", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dm", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dl", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dc", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcb", 	  1,  1,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcbn", 	  1,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcn", 	  1,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},
    {"SunOS+5", "EXB-8200", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_VERIFY_EOFS, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
       "%[^/]/st@%d,0:", "%s/st@%d,0:", 2, SunOSfind_dev, {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, pass-thru */
       { "%s/st@%d,0:ubn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:ubn", 	  0,  0,  0,    1,  0,                 0, 0, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "%s/st@%d,0:hbn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:mbn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:lbn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:ub", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:hb", 	  0,  0,  0,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:mb", 	  0,  0,  0,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:lb", 	  0,  0,  0,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:un", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:hn", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:mn", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:ln", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:u", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:h", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:m", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:l", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:c", 	  0,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:cb", 	  0,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:cbn", 	  0,  1,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:cn", 	  0,  1,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:lbn", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:n", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},
    {"SunOS+5", "EXB-85", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_VERIFY_EOFS, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
       "%[^/]/st@%d,0:", "%s/st@%d,0:", 2, SunOSfind_dev, {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru  */
       { "%s/st@%d,0:mbn", 	  1,  0, 0x15,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
    /* Usable Varirable */
       { "%s/st@%d,0:lbn", 	  0,  0, 0x14,  0,  0,FTT_RDNW|        0, 1, SUN_MAX_BLKSIZE},
    /* Densitites */
       { "%s/st@%d,0:ub", 	  0,  1, 0x90,  0,  0,FTT_RDNW| FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:hb", 	  1,  0, 0x15,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:mb", 	  1,  1, 0x8c,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:lb", 	  0,  0, 0x14,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:b", 	  1,  0, 0x15,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "%s/st@%d,0:ubn", 	  0,  1, 0x90,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:mbn", 	  1,  0, 0x15,  1,  0, FTT_RDNW|       0, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:hbn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:bn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:un", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:hn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:mn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:ln", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:u", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:h", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:m", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:l", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:c", 	  0,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:cb", 	  0,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:cbn", 	  0,  1,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:cn", 	  0,  1,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:lbn", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:n", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},
    {"SunOS+5", "DLT", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_VERIFY_EOFS, FTT_OP_GET_STATUS, ftt_trans_table, DLT_density_trans,
       "%[^/]/st@%d,0:", "%s/st@%d", 2, SunOSfind_dev, {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru  */
       { "%s/st@%d,0:ubn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:ubn", 	  5,  1, 0x1A,  0,  0,                 0, 0, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:ubn", 	  5,  0, 0x00,  1,  0,                 0, 0, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:hbn", 	  4,  0, 0x19,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:hbn", 	  4,  1, 0x19,  0,  0,                 0, 0, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:mbn", 	  3,  0, 0x18,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:lbn", 	  2,  0, 0x17,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Densitites */
       { "%s/st@%d,0:", 	  0,  0, 0x0A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  1,  0, 0x16,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  2,  0, 0x17,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  3,  0, 0x18,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  4,  0, 0x80,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  4,  1, 0x81,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  4,  0, 0x19,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  4,  1, 0x19,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  5,  0, 0x82,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  5,  1, 0x83,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  5,  1, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "%s/st@%d,0:cbn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:bn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:ub", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:cb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:hb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:mb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:lb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:b", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "%s/st@%d,0:un", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:hn", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:mn", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:ln", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:u", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:h", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:m", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:l", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:c", 	  0,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:cb", 	  0,  1,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:cbn", 	  0,  1,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:cn", 	  0,  1,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:lbn", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "%s/st@%d,0:n", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},

    {"SunOS+5", "SDX-3", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_VERIFY_EOFS, FTT_OP_GET_STATUS, ftt_trans_table, AIT_density_trans,
       "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {
    /*   string          den mod hwd   pas fxd rewind            1st */
    /*   ======          === === ===   === === ======            === */
    /* Default, passthru  */
       { "rmt/%dhbn", 	  0,  0, 0x00,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhbn", 	  0,  0, 0x00,  1,  0,                 0, 1, SUN_MAX_BLKSIZE},
    /* Usable */
       { "rmt/%dhbn", 	  0,  1, 0x00,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
    /* Densitites */
       { "rmt/%dhbn", 	  0,  0, 0x30,  0,  0,                 0, 0, SUN_MAX_BLKSIZE},
       { "rmt/%dub", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhb", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmb", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dlb", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%db", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dubn", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmbn", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dbn", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dun", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhn", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmn", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dln", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%du", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dh", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dm", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dl", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dc", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcb", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcbn", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcn", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dn", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},
    {"SunOS+5", "SDX-5", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_VERIFY_EOFS, FTT_OP_GET_STATUS, ftt_trans_table, AIT_density_trans,
       "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {
    /*   string          den mod hwd   pas fxd rewind            1st */
    /*   ======          === === ===   === === ======            === */
    /* Default, passthru  */
       { "rmt/%dhbn", 	  1,  0, 0x00,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhbn", 	  1,  0, 0x31,  1,  0,                 0, 1, SUN_MAX_BLKSIZE},
    /* Usable */
       { "rmt/%dlbn", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhbn", 	  1,  1, 0x31,  0,  0,                 0, 0, SUN_MAX_BLKSIZE},
       { "rmt/%dlbn", 	  0,  1, 0x30,  0,  0,                 0, 0, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/%dub", 	  1,  0, 0x31,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhb", 	  1,  0, 0x31,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmb", 	  1,  0, 0x31,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dlb", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%db", 	  1,  0, 0x31,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dubn", 	  1,  0, 0x31,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmbn", 	  1,  0, 0x31,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dbn", 	  1,  0, 0x31,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dun", 	  1,  0, 0x31,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhn", 	  1,  0, 0x31,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmn", 	  1,  0, 0x31,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dln", 	  0,  0, 0x30,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%du", 	  1,  0, 0x31,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dh", 	  1,  0, 0x31,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dm", 	  1,  0, 0x31,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dl", 	  0,  0, 0x30,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%d", 	  1,  0, 0x31,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dc", 	  1,  0, 0x31,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcb", 	  1,  0, 0x31,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcbn", 	  1,  0, 0x31,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dcn", 	  1,  0, 0x31,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dn", 	  1,  0, 0x31,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},
    {"SunOS+5", "", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_VERIFY_EOFS, FTT_OP_GET_STATUS, ftt_trans_table, Generic_density_trans,
       "rmt/%d", "rmt/%d", 1, SunOSfind_dev, {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru  */
       { "rmt/%dmbn", 	  1,  0, 0x00,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
    /* Usable Varirable */
       { "rmt/%dlbn", 	  0,  0, 0x00,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
    /* Densitites */
       { "rmt/%dub", 	  3,  0, 0x00,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhb", 	  2,  0, 0x00,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmb", 	  1,  0, 0x00,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dlb", 	  0,  0, 0x00,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%db", 	  1,  0, 0x00,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "rmt/%dubn", 	  3,  0, 0x00,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmbn", 	  1,  0, 0x00,  1,  0, FTT_RDNW|       0, 0, SUN_MAX_BLKSIZE},
       { "rmt/%dhbn", 	  2,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dbn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dun", 	  3,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dhn", 	  2,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dmn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dln", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%du", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dh", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dm", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "rmt/%dl", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},
    {"OSF1", "EXB-8200", "SCSI", FTT_FLAG_SUID_DRIVEID|FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
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
        { "dev/nrmt%dl",         2,  0,0x27, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descritptive */
        { "dev/rmt%dl",          2,  1,0x27, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dm",          2,  1,0x27, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dm",         2,  1,0x27, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          2,  1,0x27, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dh",         2,  1,0x27, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%da",          2,  1,0x27, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         2,  1,0x27, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* densities */
        { "dev/nrmt%da",         0,  1,0x90, 0,  0,FTT_RDNW| FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         1,  1,0x8c, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         1,  0,0x15, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         0,  0,0x14, 0,  0,FTT_RDNW| FTT_RDNW, 0, EXB_MAX_BLKSIZE},
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
        { "dev/nrmt%dl",         0,  0,0x14, 0,  0,FTT_RDNW| FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         1,  0,0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%dm",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dl",          0,  0,0x14, 0,  0,FTT_RDNW| FTT_RWOC, 1, EXB_MAX_BLKSIZE},
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
        { "dev/nrmt%dl",         0,  0,0x14, 0,  0,FTT_RDNW| FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%dm",         1,  1,0x8c, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         0,  1,0x90, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%dl",          0,  0,0x14, 0,  0, FTT_RDNW|FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dm",          1,  1,0x8c, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%da",          0,  1,0x90, 0,  0,FTT_RDNW| FTT_RWOC, 1, EXB_MAX_BLKSIZE},
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
        { "dev/nrmt%dl",         0,  0,0x14, 0,  0,FTT_RDNW| FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/nrmt%da",         1,  0,0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%dm",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dh",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%dl",          0,  0,0x14, 0,  0, FTT_RDNW|FTT_RWOC, 1, EXB_MAX_BLKSIZE},
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
    {"AIX", "EXB-8900", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"dev/rmt%d", "dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru  */
        { "dev/rmt%d.5",        2,  1, 0x27, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Variable useable */
        { "dev/rmt%d.5",        2,  0, 0x27, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        2,  1, 0x27, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Fixed useable */
        { "dev/rmt%d.5",        2,  1, 0x27, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        2,  0, 0x27, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  0,FTT_RDNW| FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  1, 0x90, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        1,  0, 0x15, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        1,  1, 0x8c, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  1,FTT_RDNW| FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  1, 0x90, 0,  1,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        1,  0, 0x15, 0,  1,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        1,  1, 0x8c, 0,  1,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
    /* Descriptive/translators */
        { "dev/rmt%d",         -1,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, 512},
        { "dev/rmt%d.1",       -1,  0, 0x00, 0,  1,          FTT_RDNW, 1, 512},
        { "dev/rmt%d.1",        0,  0, 0x14, 0,  1,FTT_RDNW| FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        1,  0, 0x15, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        1,  1, 0x8c, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        0,  1, 0x90, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        2,  0, 0x27, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        2,  1, 0x27, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.2",       -1,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, 512},
        { "dev/rmt%d.3",       -1,  0, 0x00, 0,  1,          FTT_RTOO, 1, 512},
        { "dev/rmt%d.4",        0,  0, 0x14, 0,  0, FTT_RWOC|FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        0,  0, 0x14, 0,  0, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x14, 0,  0,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "EXB-8505", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI,
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"dev/rmt%d","dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
        { "dev/rmt%d.1",        1,  0, 0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable, variable */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  0, FTT_RDNW|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  1, 0x90, 0,  0,FTT_RDNW|        0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  1, 0x8c, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable Fixed */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  1, FTT_RDNW|       0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  1, 0x90, 0,  1,FTT_RDNW|        0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  0, 0x15, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  1, 0x8c, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
   /* Descriptive */
        { "dev/rmt%d.4",        0,  0, 0x14, 0,  1, FTT_RWOC|FTT_RDNW, 1, 512},
        { "dev/rmt%d.1",        1,  0, 0x00, 0,  1,                 0, 0, 512},
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.5",        1,  0, 0x15, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        1,  1, 0x8c, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.1",        0,  1, 0x90, 0,  1,          FTT_RDNW, 0, 512},
        { "dev/rmt%d.6",        0,  0, 0x14, 0,  1, FTT_RWOC|FTT_RTOO, 1, 512},
        { "dev/rmt%d.7",        0,  0, 0x14, 0,  1,          FTT_RTOO, 1, 512},
        { "dev/rmt%d",          1,  0, 0x00, 0,  0, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.2",        1,  0, 0x00, 0,  0, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.3",        1,  0, 0x00, 0,  0,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "EXB-8500", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"dev/rmt%d","dev/rmt%d", 1, AIXfind,  {
    /*   string                den mod hwd   pas fxd rewind            1st */
    /*   ======                === === ===   === === ======            === */
    /* Default, passthru */
        { "dev/rmt%d.1",        1,  0, 0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Variable Usable */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  0,FTT_RDNW|        0, 1, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "dev/rmt%d.5",        0,  0, 0x14, 0,  1,FTT_RDNW|        0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  0, 0x15, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "dev/rmt%d.4",        0,  0, 0x14, 0,  1, FTT_RWOC|FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        0,  0, 0x14, 0,  1, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x14, 0,  1,          FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d",          1,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d",          1,  0, 0x15, 0,  1, FTT_RWOC|       0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.2",        1,  0, 0x15, 0,  1, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.3",        1,  0, 0x15, 0,  1,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "EXB-8200", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
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
    {"AIX", "DLT7", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS|FTT_OP_ERASE,ftt_trans_table_AIX, DLT_density_trans,
	"dev/rmt%d", "dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru */
        { "dev/rmt%d.5",        6,  0, 0x84, 0,  0,                          0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",       -1,  0, 0x00, 1,  0,                          0, 0, EXB_MAX_BLKSIZE},
    /* Other Densities */
        { "dev/rmt%d.5",        4,  0, 0x80, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        4,  1, 0x81, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        5,  0, 0x1A, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        5,  0, 0x82, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        5,  1, 0x83, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        6,  1, 0x85, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
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
    {"AIX", "DLT4", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
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
    {"AIX", "DLT2", "SCSI", FTT_FLAG_VERIFY_EOFS|FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
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
    {"AIX", "SDX-3", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, AIT_density_trans,
	"dev/rmt%d", "dev/rmt%d", 1, AIXfind,  {
    /*   string                den mod hwd   pas fxd rewind            1st */
    /*   ======                === === ===   === === ======            === */
    /* Default, Passthru  */
        { "dev/rmt%d.1",        0,  0, 0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Variable useable */
        { "dev/rmt%d.1",        0,  0, 0x30, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        0,  1, 0x30, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  0, 0x30, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Fixed useable */
        { "dev/rmt%d.1",        0,  0, 0x30, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  0, 0x30, 0,  1,                 0, 1, EXB_MAX_BLKSIZE},
    /* descriptive */
        { "dev/rmt%d",          0,  0, 0x30, 0,  0,FTT_RWOC|        0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.2",        0,  0, 0x30, 0,  0,FTT_RTOO|FTT_RWOC , 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.3",        0,  0, 0x30, 0,  0,FTT_RTOO|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.4",        0,  0, 0x30, 0,  0,FTT_RWOC|        0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        0,  0, 0x30, 0,  0,FTT_RTOO|FTT_RWOC , 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x30, 0,  0,FTT_RTOO|        0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d",          0,  0, 0x30, 0,  1,FTT_RWOC|        0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.2",        0,  0, 0x30, 0,  1,FTT_RTOO|FTT_RWOC , 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.3",        0,  0, 0x30, 0,  1,FTT_RTOO|       0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.4",        0,  0, 0x30, 0,  1,FTT_RWOC|        0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        0,  0, 0x30, 0,  1,FTT_RTOO|FTT_RWOC , 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x30, 0,  1,FTT_RTOO|        0, 0, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "SDX-5", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, AIT_density_trans,
	"dev/rmt%d", "dev/rmt%d", 1, AIXfind,  {
    /*   string                den mod hwd   pas fxd rewind            1st */
    /*   ======                === === ===   === === ======            === */
    /* Default, Passthru  */
        { "dev/rmt%d.1",        1,  0, 0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Variable useable */
        { "dev/rmt%d.1",        1,  1, 0x31, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  0, 0x30, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  1, 0x30, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Fixed useable */
        { "dev/rmt%d.1",        1,  0, 0x31, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  0, 0x30, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.1",        1,  1, 0x31, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.5",        0,  1, 0x30, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* descriptive */
        { "dev/rmt%d",          1,  0, 0x31, 0,  0,FTT_RWOC|        0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.2",        1,  0, 0x31, 0,  0,FTT_RTOO|FTT_RWOC , 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.3",        1,  0, 0x31, 0,  0,FTT_RTOO|       0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.4",        0,  0, 0x30, 0,  0,FTT_RWOC|        0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        0,  0, 0x30, 0,  0,FTT_RTOO|FTT_RWOC , 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x30, 0,  0,FTT_RTOO|        0, 1, EXB_MAX_BLKSIZE},
        { "dev/rmt%d",          1,  0, 0x31, 0,  1,FTT_RWOC|        0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.2",        1,  0, 0x31, 0,  1,FTT_RTOO|FTT_RWOC , 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.3",        1,  0, 0x31, 0,  1,FTT_RTOO|       0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.4",        0,  0, 0x30, 0,  1,FTT_RWOC|        0, 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.6",        0,  0, 0x30, 0,  1,FTT_RTOO|FTT_RWOC , 0, EXB_MAX_BLKSIZE},
        { "dev/rmt%d.7",        0,  0, 0x30, 0,  1,FTT_RTOO|        0, 0, EXB_MAX_BLKSIZE},
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
    {"IRIX", "AIT","SCSI",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, AIT_density_trans,
	"%*[rmt]/tps%dd%d%*[nrsv.]","rmt/tps%dd%d", 2, IRIXfind,  {
	    /*   string             den mod hwd  pas fxd rewind        sf,1st */
	    /*   ======             === === ===  === === ======        ==  = */
    /* Default, Passthru */
	{ "rmt/tps%dd%dnrv",         1,  0,0x31, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "scsi/sc%dd%dl0",         -1,  0,  -1, 1,  0,                 0, 1, EXB_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrvc",        1,  1,0x31, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Variable */
	{ "rmt/tps%dd%dnrnsv",       1,  0,0x31, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsvc",      1,  1,0x31, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "rmt/tps%dd%dnr",          1,  0,0x31, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrc",         1,  1,0x31, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns",        1,  0,0x31, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsc",       1,  1,0x31, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "rmt/tps%dd%dstat",       -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            1,  0,0x31, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dc",           1,  1,0x31, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr",          1,  0,0x31, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrc",         1,  1,0x31, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs",         1,  0,0x31, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsc",        1,  1,0x31, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv",        1,  0,0x31, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsvc",       1,  1,0x31, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns",          1,  0,0x31, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsc",         1,  1,0x31, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsvc",        1,  1,0x31, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv",         1,  0,0x31, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds",           1,  0,0x31, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsvc",         1,  1,0x31, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv",          1,  0,0x31, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dvc",          1,  1,0x31, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv",           1,  0,0x31, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
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
	{ "rmt/tps%dd%d",            0,  1,0x90, 0,  1,FTT_RDNW| FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            0,  0,0x14, 0,  1,FTT_RDNW| FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            1,  1,0x8c, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
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
	{ "rmt/tps%dd%dnrnsv.8200",  0,  0,0x14, 0,  0,FTT_RDNW|        0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8200c", 0,  1,0x8C, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8500c", 0,  1,0x90, 0,  0,FTT_RDNW|        0, 1, IRIX_MAX_BLKSIZE},
     /* Usable Fixed */
	{ "rmt/tps%dd%dnrns.8200",   0,  0,0x14, 0,  1,FTT_RDNW|        0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns.8200c",  0,  1,0x8C, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns.8500",   1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns.8500c",  0,  1,0x90, 0,  1, FTT_RDNW|       0, 1, IRIX_MAX_BLKSIZE},
     /* Descriptive */
	{ "rmt/tps%dd%dstat",       -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d",            1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d.8200",       0,  0,0x8C, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d.8200c",      0,  0,0x14, 0,  1,FTT_RDNW| FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d.8500",       1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%d.8500c",      1,  0,0x90, 0,  1,FTT_RDNW| FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr",          1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr.8200",     0,  0,0x14, 0,  1,FTT_RDNW|        0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr.8200c",    0,  1,0x8C, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr.8500",     1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnr.8500c",    0,  1,0x90, 0,  1,FTT_RDNW|        0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrns",        1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv",       1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8200",  0,  0,0x14, 0,  0,FTT_RDNW|        0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8200c", 0,  1,0x8C, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8500",  1,  0,0x15, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrnsv.8500c", 0,  1,0x90, 0,  0, FTT_RDNW|       0, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs",         1,  0,0x15, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs.8200",    0,  0,0x14, 0,  1,FTT_RDNW| FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs.8200c",   0,  1,0x8C, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs.8500",    1,  0,0x15, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrs.8500c",   0,  1,0x90, 0,  1,FTT_RDNW| FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv",        1,  0,0x15, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv.8200",   0,  0,0x14, 0,  0,FTT_RDNW| FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv.8200c",  0,  1,0x8C, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv.8500",   1,  0,0x15, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrsv.8500c",  0,  1,0x90, 0,  0, FTT_RDNW|FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv",         1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv.8200",    1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv.8200c",   1,  1,0x8C, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv.8500",    1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnrv.8500c",   0,  1,0x90, 0,  0,FTT_RDNW|        0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns",          1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns.8200",     0,  0,0x14, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns.8200c",    0,  1,0x8C, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns.8500",     1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dns.8500c",    0,  1,0x90, 0,  1, FTT_RDNW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv",         1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv.8200",    0,  0,0x14, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv.8200c",   0,  1,0x8C, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv.8500",    1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dnsv.8500c",   0,  1,0x90, 0,  0,FTT_RDNW| FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds",           1,  0,0x15, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds.8200",      0,  0,0x14, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds.8200c",     0,  1,0x8C, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds.8500",      1,  0,0x15, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%ds.8500c",     0,  1,0x90, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv",          1,  0,0x15, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv.8200",     0,  0,0x14, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv.8200c",    0,  1,0x8C, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv.8500",     1,  0,0x15, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dsv.8500c",    0,  1,0x90, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv",           1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv.8200",      0,  0,0x14, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv.8200c",     0,  1,0x8C, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv.8500",      1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/tps%dd%dv.8500c",     0,  1,0x90, 0,  0,FTT_RDNW| FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
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
	{ "rmt/jag%dd%dnrv.8200",    0,  0,0x14, 0,  0,FTT_RDNW|        0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "rmt/jag%dd%dnr.8500",     1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%dnr.8200",     0,  0,0x14, 0,  1,FTT_RDNW|        0, 1, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "rmt/jag%dd%dstat",       -1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d.8200",       0,  0,0x14, 0,  1,FTT_RDNW| FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
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
	{ "rmt/jag%dd%d",            0,  1,0x90, 0,  1,FTT_RDNW| FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            0,  0,0x14, 0,  1,FTT_RDNW| FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "rmt/jag%dd%d",            1,  1,0x8c, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
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
    {"2SDX",
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px0f|FTT_DO_RS|
	FTT_DO_SN|FTT_DO_LS|FTT_DO_RP},

    {"1EXB-8200", 
	FTT_DO_VSRS|
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|FTT_DO_EXBRS|
	FTT_DO_EXB82FUDGE},

    {"2EXB-8200", 
	FTT_DO_VSRS|
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|FTT_DO_EXBRS|
	FTT_DO_EXB82FUDGE},

    {"2EXB-8510", 
	FTT_DO_VSRS|
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|
	FTT_DO_EXBRS|FTT_DO_SN|FTT_DO_RP_SOMETIMES},

    {"1EXB-8500", 
	FTT_DO_VSRS|
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|
	FTT_DO_EXBRS|FTT_DO_SN|FTT_DO_RP_SOMETIMES},

    {"2EXB-8500", 
	FTT_DO_VSRS|
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|
	FTT_DO_EXBRS|FTT_DO_SN|FTT_DO_RP_SOMETIMES},

    {"2EXB-8505", 
	FTT_DO_VSRS|
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px20_EXB|FTT_DO_RS|
	FTT_DO_EXBRS| FTT_DO_05RS|FTT_DO_SN|FTT_DO_LS|
	FTT_DO_RP_SOMETIMES},

    {"2EXB-8205", 
	FTT_DO_VSRS|
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px20_EXB|FTT_DO_RS|
	FTT_DO_EXBRS| FTT_DO_05RS|FTT_DO_SN|FTT_DO_LS|
	FTT_DO_RP_SOMETIMES},

    {"2EXB-8900", 
	FTT_DO_VSRS|
        FTT_DO_MS|
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_RS|
	FTT_DO_EXBRS|FTT_DO_05RS|FTT_DO_SN|
	FTT_DO_LS|FTT_DO_RP_SOMETIMES|FTT_DO_MS_Px21},

    {"2DLT",      
	FTT_DO_VSRS|
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px10|FTT_DO_RS|
	FTT_DO_DLTRS|FTT_DO_SN|FTT_DO_LS|FTT_DO_RP},

    {"2TZ8",      
	FTT_DO_VSRS|
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px0f|FTT_DO_RS|
	FTT_DO_DLTRS|FTT_DO_SN|FTT_DO_LS|FTT_DO_RP},

    {"2", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px0f|FTT_DO_RS|
	FTT_DO_SN|FTT_DO_LS|FTT_DO_RP},

    {"1", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS },

    {"", 0},

    {0,0}
};
