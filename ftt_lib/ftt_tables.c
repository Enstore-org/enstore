#include <stdio.h>
#include <ftt_private.h>

int ftt_trans_open[MAX_TRANS_ERRNO] = {
	/*    0 NOERROR	*/	FTT_SUCCESS,
	/*    1	EPERM	*/	FTT_EPERM,
	/*    2	ENOENT	*/	FTT_ENOENT,
	/*    3	ESRCH	*/	FTT_ENOENT,
	/*    4	EINTR	*/	FTT_ENOENT,
	/*    5	EIO	*/	FTT_EIO,
	/*    6	ENXIO	*/	FTT_EFAULT,
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
	/*   19	ENODEV	*/	FTT_ENOENT,
	/*   20	ENOTDIR	*/	FTT_ENOENT,
	/*   21	EISDIR	*/	FTT_ENOENT,
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
};

int *ftt_trans_table_AIX[] = {
    /* none...but just to be safe0 */ ftt_trans_in,
    /* FTT_OPN_READ		 1 */ ftt_trans_in_AIX,
    /* FTT_OPN_WRITE		 2 */ ftt_trans_out_AIX,
    /* FTT_OPN_WRITEFM		 3 */ ftt_trans_out_AIX,
    /* FTT_OPN_SKIPREC		 4 */ ftt_trans_skiprec,
    /* FTT_OPN_SKIPFM		 5 */ ftt_trans_skipf_AIX,
    /* FTT_OPN_REWIND		 6 */ ftt_trans_skipr_AIX,
    /* FTT_OPN_UNLOAD		 7 */ ftt_trans_skipf,
    /* FTT_OPN_RETENSION	 8 */ ftt_trans_skipf,
    /* FTT_OPN_ERASE		 9 */ ftt_trans_out,
    /* FTT_OPN_STATUS		10 */ ftt_trans_in,
    /* FTT_OPN_GET_STATUS	11 */ ftt_trans_in,
    /* FTT_OPN_ASYNC 		12 */ ftt_trans_in,
    /* FTT_OPN_PASSTHRU         13 */ ftt_trans_out,
    /* FTT_OPN_CHALL            14 */ ftt_trans_chall,
    /* FTT_OPN_OPEN             15 */ ftt_trans_open,
    /* FTT_OPN_RSKIPREC		16 */ ftt_trans_skiprec,
    /* FTT_OPN_RSKIPFM		16 */ ftt_trans_skipr_AIX,
};
int *ftt_trans_table[] = {
    /* none...but just to be safe0 */ ftt_trans_in,
    /* FTT_OPN_READ		 1 */ ftt_trans_in,
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
    /* FTT_OPN_PASSTHRU         13 */ ftt_trans_out,
    /* FTT_OPN_CHALL            14 */ ftt_trans_chall,
    /* FTT_OPN_OPEN             15 */ ftt_trans_open,
    /* FTT_OPN_RSKIPREC		16 */ ftt_trans_skiprec,
    /* FTT_OPN_RSKIPFM		16 */ ftt_trans_skipr,
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
   "IFS=\" \t\n\";\
    PATH=\"/bin:/usr/bin:/sbin:/usr/sbin\";\
    dnum=%d\n\
    entry=0\n\
    uerf -R -r 300 | \n\
	while read line\n\
	do\n\
	    if [ $dnum = -1 ]\n\
	    then\n\
		echo $line | sed -e 's/.*[      ]\\([^ ][^ ]*\\)  *\\([^ ][^ ]*\\)).*/\\1/'\n\
		exit\n\
	    fi\n\
	    case \"$line\" in\n\
	    \\*\\*\\*\\**) 	entry=`expr $entry + 1`;;\n\
	    tz*)  		dnum=`expr $dnum - 1` ;;\n\
	    esac\n\
	    if [ $entry -gt 1 ]\n\
	    then\n\
		exit\n\
	    fi\n\
	done";

static char SunOSfind[] = 
   "IFS=\" \t\n\";\
    PATH=\"/bin:/usr/bin:/sbin:/usr/sbin\";\
    drive=%d\n\
    id=\"`ls -l /dev/rmt/${drive} | sed -e 's;.*/\\(.*\\)@\\([0-9]*\\).*;\\1\\2;'`\" \n\
    line=\"`dmesg | grep \\^${id}: `\" \n\
    case \"$line\" in \n\
    *Vendor*)  \n\
	echo \"$line\" | sed -e 's/.*Product..\\([^ ]*\\).*>/\\1/' \n\
	;; \n\
    *) \n\
	echo \"$line\" | sed -e 's/[^<]*<[^ ]* \\([^ ]*\\).*>/\\1/' \n\
	;; \n\
    esac \n";
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
#define SUN_MAX_BLKSIZE 65534
ftt_dev_entry devtable[] = {
    {"SunOS+5", "EXB-8200", "SCSI", FTT_FLAG_SUID_SCSI, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
       "/dev/rmt/%d", "/dev/rmt/%d", 1, SunOSfind, {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, pass-thru */
       { "/dev/rmt/%dubn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dubn", 	  0,  0,  0,    1,  0,                 0, 1, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "/dev/rmt/%dhbn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dmbn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dlbn", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dub", 	  0,  0,  0,    0,  0,                 0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dhb", 	  0,  0,  0,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dmb", 	  0,  0,  0,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dlb", 	  0,  0,  0,    0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dun", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dhn", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dmn", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dln", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%du", 	  0,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dh", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dm", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dl", 	  0,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},
    {"SunOS+5", "EXB-85", "SCSI", FTT_FLAG_SUID_SCSI, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
       "/dev/rmt/%d", "/dev/rmt/%d", 1, SunOSfind, {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru  */
       { "/dev/rmt/%dmbn", 	  1,  0, 0x15,  0,  0,                 0, 0, SUN_MAX_BLKSIZE},
    /* Usable Varirable */
       { "/dev/rmt/%dlbn", 	  0,  0, 0x14,  0,  0,                 0, 1, SUN_MAX_BLKSIZE},
    /* Densitites */
       { "/dev/rmt/%dub", 	  1,  1, 0x90,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dhb", 	  1,  0, 0x15,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dmb", 	  0,  1, 0x8c,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dlb", 	  0,  0, 0x14,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%db", 	  1,  0, 0x15,  0,  0,          FTT_RWOC, 1, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "/dev/rmt/%dubn", 	  1,  1, 0x90,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dmbn", 	  1,  0, 0x15,  1,  0, FTT_RDNW|       0, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dhbn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dbn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dun", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dhn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dmn", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dln", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%du", 	  1,  0,  0,    0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dh", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dm", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dl", 	  1,  0,  0,    0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},
    {"SunOS+5", "DLT", "SCSI", FTT_FLAG_SUID_SCSI, FTT_OP_GET_STATUS, ftt_trans_table, DLT_density_trans,
       "/dev/rmt/%d", "/dev/rmt/%d", 1, SunOSfind, {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru  */
       { "/dev/rmt/%dubn", 	  5,  0, 0x1A,  0,  0,                 0, 0, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dubn", 	  5,  1, 0x1A,  0,  0,                 0, 0, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dubn", 	  5,  0, 0x00,  1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable */
       { "/dev/rmt/%dubn", 	  5,  0, 0x1A,  0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Densitites */
       { "/dev/rmt/%d", 	  0,  0, 0x0A,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  1,  0, 0x16,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  2,  0, 0x17,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  3,  0, 0x18,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  4,  0, 0x80,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  4,  1, 0x81,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  4,  0, 0x19,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  4,  1, 0x19,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  5,  0, 0x82,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  5,  1, 0x83,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%d", 	  5,  1, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 0, SUN_MAX_BLKSIZE},
    /* Descriptive */
       { "/dev/rmt/%dubn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dcbn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dhbn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dmbn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dlbn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dbn", 	  5,  0, 0x1A,  0,  0,                 0, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dub", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dcb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dhb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dmb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dlb", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%db", 	  5,  0, 0x1A,  0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
       { "/dev/rmt/%dun", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dhn", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dmn", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dln", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%du", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|       0, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dh", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dm", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { "/dev/rmt/%dl", 	  5,  0, 0x1A,  0,  0, FTT_RDNW|FTT_RWOC, 1, SUN_MAX_BLKSIZE},
       { 0 },
    }},
    {"OSF1", "EXB-8200", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"/dev/%*[nr]mt%d","/dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru  */
        { "/dev/nrmt%dl",         0,  0,0x00, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dl",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descritptive */
        { "/dev/rmt%dl",          0,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dl",         0,  0,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dm",          0,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dm",         0,  0,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          0,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dh",         0,  0,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%da",          0,  0,0x00, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%da",         0,  0,0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
    {"OSF1", "EXB-8500", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"/dev/%*[nr]mt%d","/dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru */
        { "/dev/nrmt%dh",         1,  0,0x15, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dh",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable */
        { "/dev/nrmt%dl",         0,  0,0x14, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dm",         1,  0,0x14, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dh",         1,  0,0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%da",         1,  0,0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "/dev/rmt%dm",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dl",          0,  0,0x14, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%da",          1,  0,0x14, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
    {"OSF1", "EXB-8505", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"/dev/%*[nr]mt%d","/dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru */
        { "/dev/nrmt%dh",         1,  0,0x15, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dh",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable */
        { "/dev/nrmt%dl",         0,  0,0x14, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dm",         0,  1,0x8c, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dh",         1,  0,0x15, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%da",         1,  1,0x90, 0,  0,          FTT_RDNW, 1, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "/dev/rmt%dl",          0,  0,0x14, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dm",          0,  1,0x8c, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          1,  0,0x15, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%da",          1,  1,0x90, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
    {"OSF1", "DLT", "SCSI", FTT_FLAG_SUID_SCSI|FTT_FLAG_BSIZE_AFTER, FTT_OP_GET_STATUS, ftt_trans_table, DLT_density_trans,
	"/dev/%*[nr]mt%d","/dev/rmt%d", 1, OSF1find,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru */
        { "/dev/nrmt%dh",         5,  0,0x1A, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dh",        -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable */
        { "/dev/nrmt%dl",         4,  0,0x18, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dm",         4,  1,0x19, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%dh",         5,  0,0x1A, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/nrmt%da",         5,  1,0x1A, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Other Densities */
        { "/dev/rmt%dh",          0,  0,0x0A, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          1,  0,0x16, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          2,  0,0x17, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          3,  0,0x18, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          4,  0,0x19, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          5,  0,0x1A, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          5,  0,0x80, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          5,  1,0x81, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          5,  0,0x82, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          5,  1,0x83, 0,  0,          FTT_RWOC, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "/dev/rmt%dh",          5,  0,0x82, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dl",          4,  0,0x19, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dm",          4,  1,0x19, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%dh",          5,  0,0x1A, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%da",          5,  1,0x1A, 0,  0,          FTT_RWOC, 1, EXB_MAX_BLKSIZE},
	{ 0 },
     }},
    {"AIX", "EXB-8900", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"/dev/rmt%d", "/dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, Passthru  */
        { "/dev/rmt%d.5",        2,  0, 0x27, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Variable useable */
        { "/dev/rmt%d.5",        0,  0, 0x14, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        0,  1, 0x90, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  0, 0x15, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  1, 0x8c, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        2,  0, 0x27, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        2,  1, 0x27, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Fixed useable */
        { "/dev/rmt%d.5",        0,  0, 0x14, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        0,  1, 0x90, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  0, 0x15, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  1, 0x8c, 0,  0,          FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        2,  0, 0x27, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        2,  1, 0x27, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive/translators */
        { "/dev/rmt%d",         -1,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, 512},
        { "/dev/rmt%d.1",       -1,  0, 0x00, 0,  1,                 0, 1, 512},
        { "/dev/rmt%d.2",       -1,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, 512},
        { "/dev/rmt%d.3",       -1,  0, 0x00, 0,  1,          FTT_RTOO, 1, 512},
        { "/dev/rmt%d.4",        0,  0, 0x14, 0,  0, FTT_RWOC|FTT_RDNW, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.6",        0,  0, 0x14, 0,  0, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.7",        0,  0, 0x14, 0,  0,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "EXB-8505", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"/dev/rmt%d","/dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
        { "/dev/rmt%d.5",        1,  0, 0x15, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable, variable */
        { "/dev/rmt%d.5",        0,  0, 0x14, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        0,  1, 0x90, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  0, 0x15, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  1, 0x8c, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Usable Fixed */
        { "/dev/rmt%d.5",        0,  0, 0x14, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        0,  1, 0x90, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  0, 0x15, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  1, 0x8c, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
   /* Descriptive */
        { "/dev/rmt%d",         -1,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, 512},
        { "/dev/rmt%d.1",       -1,  0, 0x00, 0,  1,                 0, 1, 512},
        { "/dev/rmt%d.2",       -1,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, 512},
        { "/dev/rmt%d.3",       -1,  0, 0x00, 0,  1,          FTT_RTOO, 1, 512},
        { "/dev/rmt%d.4",        0,  0, 0x14, 0,  0, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.6",        0,  0, 0x14, 0,  0, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.7",        0,  0, 0x14, 0,  0,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "EXB-8500", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"/dev/rmt%d","/dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru */
        { "/dev/rmt%d.5",        1,  0, 0x15, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Variable Usable */
        { "/dev/rmt%d.5",        0,  0, 0x14, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  0, 0x15, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "/dev/rmt%d.5",        0,  0, 0x14, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  0, 0x15, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "/dev/rmt%d",         -1,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, 512},
        { "/dev/rmt%d.1",       -1,  0, 0x00, 0,  1,                 0, 1, 512},
        { "/dev/rmt%d.2",       -1,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, 512},
        { "/dev/rmt%d.3",       -1,  0, 0x00, 0,  1,          FTT_RTOO, 1, 512},
        { "/dev/rmt%d.4",        0,  0, 0x14, 0,  1, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.6",        0,  0, 0x14, 0,  1, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.7",        0,  0, 0x14, 0,  1,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "EXB-8200", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
FTT_OP_STATUS|FTT_OP_GET_STATUS,ftt_trans_table_AIX, Exabyte_density_trans,
	"/dev/rmt%d","/dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default */
        { "/dev/rmt%d.5",        0,  0, 0x00, 0,  0,                 0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        0,  0, 0x00, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",       -1,  0,   -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Variable Usable */
        { "/dev/rmt%d.5",        0,  0, 0x00, 0,  0,                 0, 0, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "/dev/rmt%d.5",        0,  0, 0x00, 0,  1,                 0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "/dev/rmt%d",          0,  0, 0x00, 0,  1, FTT_RWOC|       0, 0, 512},
        { "/dev/rmt%d.1",        0,  0, 0x00, 0,  1,                 0, 0, 512},
        { "/dev/rmt%d.2",        0,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 0, 512},
        { "/dev/rmt%d.3",        0,  0, 0x00, 0,  1,          FTT_RTOO, 0, 512},
        { "/dev/rmt%d.4",        0,  0, 0x00, 0,  1, FTT_RWOC|       0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.6",        0,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.7",        0,  0, 0x00, 0,  1,        0|FTT_RTOO, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "DLT4", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS|FTT_OP_ERASE,ftt_trans_table_AIX, DLT_density_trans,
	"/dev/rmt%d", "/dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind            1st */
    /*   ======                  === === ===   === === ======            === */
    /* Default, passthru */
        { "/dev/rmt%d.5",        5,  0, 0x1A, 0,  0,                          0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",       -1,  0, 0x00, 1,  0,                          0, 0, EXB_MAX_BLKSIZE},
    /* Other Densities */
        { "/dev/rmt%d.5",        4,  0, 0x80, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        4,  1, 0x81, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        5,  0, 0x82, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        5,  1, 0x83, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        0,  0, 0x0A, 0,  0,                   FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  0, 0x16, 0,  0,                   FTT_RDNW, 0, EXB_MAX_BLKSIZE},
    /* Variable useable */
        { "/dev/rmt%d.5",        5,  0, 0x1A, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        5,  1, 0x1A, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        4,  0, 0x19, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        4,  1, 0x19, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        3,  0, 0x18, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        2,  0, 0x17, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "/dev/rmt%d.5",        5,  0, 0x1A, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        4,  0, 0x19, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        5,  1, 0x1A, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        4,  1, 0x19, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        3,  0, 0x18, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        2,  0, 0x17, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "/dev/rmt%d",         -1,  0, 0x00, 0,  1, FTT_RWOC|       0, 	 1, 512},
        { "/dev/rmt%d.1",       -1,  0, 0x00, 0,  1,                 0, 	 1, 512},
        { "/dev/rmt%d.2",       -1,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 	 1, 512},
        { "/dev/rmt%d.3",       -1,  0, 0x00, 0,  1,          FTT_RTOO, 	 1, 512},
        { "/dev/rmt%d.4",        5,  0, 0x1A, 0,  0, FTT_RWOC|       0|       0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.6",        5,  0, 0x1A, 0,  0, FTT_RWOC|FTT_RTOO|       0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.7",        5,  0, 0x1A, 0,  0,          FTT_RTOO|       0, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"AIX", "DLT2", "SCSI", FTT_FLAG_HOLD_SIGNALS|FTT_FLAG_SUID_SCSI, 
	FTT_OP_STATUS|FTT_OP_GET_STATUS|FTT_OP_ERASE,ftt_trans_table_AIX, DLT_density_trans,
	"/dev/rmt%d","/dev/rmt%d", 1, AIXfind,  {
    /*   string                  den mod hwd   pas fxd rewind                   1st blk */
    /*   ======                  === === ===   === === ======                   === === */
    /* Default, passthru */
        { "/dev/rmt%d.5",        4,  0, 0x19, 0,  0,                          0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",       -1,  0, 0x00, 1,  0,                          0, 0, EXB_MAX_BLKSIZE},
    /* Variable useable */
        { "/dev/rmt%d.5",        4,  0, 0x19, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        4,  1, 0x19, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        3,  0, 0x18, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        2,  0, 0x17, 0,  0,                          0, 0, EXB_MAX_BLKSIZE},
    /* Fixed Usable */
        { "/dev/rmt%d.5",        4,  0, 0x19, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        4,  1, 0x19, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        3,  0, 0x18, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        2,  0, 0x17, 0,  1,                          0, 0, EXB_MAX_BLKSIZE},
    /* Other Densities */
        { "/dev/rmt%d.5",        0,  0, 0x0A, 0,  0,                   FTT_RDNW, 0, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.5",        1,  0, 0x16, 0,  0,                   FTT_RDNW, 0, EXB_MAX_BLKSIZE},
    /* Descriptive */
        { "/dev/rmt%d",         -1,  0, 0x00, 0,  1, FTT_RWOC|       0, 	 1, 512},
        { "/dev/rmt%d.1",       -1,  0, 0x00, 0,  1,                 0, 	 1, 512},
        { "/dev/rmt%d.2",       -1,  0, 0x00, 0,  1, FTT_RWOC|FTT_RTOO, 	 1, 512},
        { "/dev/rmt%d.3",       -1,  0, 0x00, 0,  1,          FTT_RTOO, 	 1, 512},
        { "/dev/rmt%d.4",        4,  0, 0x19, 0,  0, FTT_RWOC|       0|       0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.6",        4,  0, 0x19, 0,  0, FTT_RWOC|FTT_RTOO|       0, 1, EXB_MAX_BLKSIZE},
        { "/dev/rmt%d.7",        4,  0, 0x19, 0,  0,          FTT_RTOO|       0, 1, EXB_MAX_BLKSIZE},
        { 0, },
    }},
    {"IRIX+5", "DLT", "SCSI", FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS|FTT_OP_ERASE, ftt_trans_table, DLT_density_trans,
	"/dev/rmt/tps%dd%d","/dev/rmt/tps%dd%d", 2, IRIXfind, {
    /*   string                    den mod hwd   pas fxd rewind            ,1st*/
    /*   ======                    === === ===   === === ======            === */
    /* Default, passthru */
	{ "/dev/rmt/tps%dd%dnrv",   5,  0,0x1A,   0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/scsi/sc%dd%dl0",   -1,  0,  -1,   1,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Usable Variable */
	{ "/dev/rmt/tps%dd%dnrv",   5,  0,0x1A, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrnsvc",5,  1,0x1A, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrv",   4,  0,0x1A, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrnsvc",4,  1,0x1A, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrv",   3,  0,0x1A, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "/dev/rmt/tps%dd%dnr",    5,  0,0x1A, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrc",   5,  1,0x1A, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnr",    4,  0,0x1A, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrc",   4,  1,0x1A, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnr",    3,  0,0x1A, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
    /* Other Densities */
	{ "/dev/rmt/tps%dd%d",      4,  0,0x80, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",      4,  1,0x81, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",      5,  0,0x82, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",      5,  1,0x83, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",      0,  0,0x0A, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",      1,  0,0x16, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",      2,  0,0x17, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",      3,  0,0x18, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",      4,  0,0x19, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",      5,  0,0x1A, 0,  1,          FTT_RDNW, 0, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "/dev/rmt/tps%dd%dstat", -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrc",   5,  1,0x1A, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrns",  5,  0,0x1A, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrnsc", 5,  1,0x1A, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrnsv", 5,  0,0x1A, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrs" ,  5,  0,0x1A, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrs" ,  5,  0,0x1A, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrsc" , 5,  1,0x1A, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrsv",  5,  0,0x1A, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrsvc", 5,  1,0x1A, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dns",    5,  0,0x1A, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnsc",   5,  1,0x1A, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnsv",   5,  0,0x1A, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnsvc",  5,  1,0x1A, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%ds",     5,  0,0x1A, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dsc",    5,  1,0x1A, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dsv",    5,  0,0x1A, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dsv",    5,  0,0x1A, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dsvc",   5,  1,0x1A, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dv",     5,  0,0x1A, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dvc",    5,  1,0x1A, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
        { 0,},
    }},
    {"IRIX+5", "EXB-85", "SCSI", FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"/dev/rmt/tps%dd%d","/dev/rmt/tps%dd%d",  2, IRIXfind,  {
	    /*   string                  den mod hwd   pas fxd rewind            1st */
	    /*   ======                  === === ===   === === ======            === */
     /* Default, passthru */
	{ "/dev/rmt/tps%dd%dnrv.8500",    1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/scsi/sc%dd%dl0",         -1,  0,  -1, 1,  0,                 0, 0, EXB_MAX_BLKSIZE},
     /* Usable Variable */
	{ "/dev/rmt/tps%dd%dnrv.8500",    1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrv.8200",    0,  0,0x14, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
     /* Usable Fixed */
	{ "/dev/rmt/tps%dd%dnr.8500",     1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnr.8200",     0,  0,0x14, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
     /* Descriptive */
	{ "/dev/rmt/tps%dd%dstat",       -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",            1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d.8200",       0,  0,0x14, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d.8500",       1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnr",          1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnr.8200",     0,  0,0x14, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnr.8500",     1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrns",        1,  0,0x15, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrns.8200",   0,  0,0x14, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrns.8500",   1,  0,0x15, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrnsv",       1,  0,0x15, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrnsv.8200",  0,  0,0x14, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrnsv.8500",  1,  0,0x15, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrs",         1,  0,0x15, 0,  1,          FTT_BTSW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrs.8200",    0,  0,0x14, 0,  1,          FTT_BTSW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrs.8500",    1,  0,0x15, 0,  1,          FTT_BTSW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrsv",        1,  0,0x15, 0,  0,          FTT_BTSW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrsv.8200",   0,  0,0x14, 0,  0,          FTT_BTSW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrsv.8500",   1,  0,0x15, 0,  0,          FTT_BTSW, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrv",         1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrv.8200",    0,  0,0x14, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrv.8500",    1,  0,0x15, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dns",          1,  0,0x15, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dns.8200",     0,  0,0x14, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dns.8500",     1,  0,0x15, 0,  1,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnsv",         1,  0,0x15, 0,  0,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnsv.8200",    0,  0,0x14, 0,  0,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnsv.8500",    1,  0,0x15, 0,  0,          FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%ds",           1,  0,0x15, 0,  1, FTT_BTSW|FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%ds.8200",      0,  0,0x14, 0,  1, FTT_BTSW|FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%ds.8500",      1,  0,0x15, 0,  1, FTT_BTSW|FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dsv",          1,  0,0x15, 0,  0, FTT_BTSW|FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dsv.8200",     0,  0,0x14, 0,  0, FTT_BTSW|FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dsv.8500",     1,  0,0x15, 0,  0, FTT_BTSW|FTT_RWOC, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dv",           1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dv.8200",      0,  0,0x14, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dv.8500",      1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
        { 0,},
    }},
    {"IRIX+5", "EXB-82","SCSI",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"/dev/rmt/tps%dd%d","/dev/rmt/tps%dd%d", 2, IRIXfind,  {
	    /*   string                  den mod hwd  pas fxd rewind            sf,1st */
	    /*   ======                  === === ===  === === ======            === */
    /* Default, Passthru */
	{ "/dev/rmt/tps%dd%dnrv",         0,  0,   0, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/scsi/sc%dd%dl0",         -1,  0,  -1, 1,  0,                 0, 1, EXB_MAX_BLKSIZE},
    /* Usable Variable */
	{ "/dev/rmt/tps%dd%dnrnsv",       0,  0,   0, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "/dev/rmt/tps%dd%dnrns",        0,  0,   0, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "/dev/rmt/tps%dd%dstat",       -1,  0,  -1, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%d",            0,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnr",          0,  0,   0, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnr",          0,  0,   0, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrns",        0,  0,   0, 0,  1,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrs",         0,  0,   0, 0,  1,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrsv",        0,  0,   0, 0,  0,          FTT_BTSW, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnrv",         0,  0,   0, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dns",          0,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dnsv",         0,  0,   0, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%ds",           0,  0,   0, 0,  1, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dsv",          0,  0,   0, 0,  0, FTT_BTSW|FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/tps%dd%dv",           0,  0,   0, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
    {"IRIX+5", "EXB-85", "JAG", FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"/dev/rmt/jag%dd%d","/dev/rmt/jag%dd%d", 2, IRIXfindVME, {
	    /*   string                  den mod hwd pas fxd rewind            1st */
	    /*   ======                  === === === === === ======            === */
    /* Default, passthru */
	{ "/dev/rmt/jag%dd%dnrv.8500",    1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/scsi/jag%dd%dl0",        -1,  0,  -1, 1,  0,                 0, 0, IRIX_MAX_BLKSIZE},
    /* Usable Variable */
	{ "/dev/rmt/jag%dd%dnrv.8500",    1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dnrv.8200",    0,  0,0x14, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Usable Fixed */
	{ "/dev/rmt/jag%dd%dnrv.8500",    1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dnrv.8200",    0,  0,0x14, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
    /* Descriptive */
	{ "/dev/rmt/jag%dd%dstat",       -1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%d",            1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%d.8200",       0,  0,0x14, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%d.8500",       1,  0,0x15, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dnr",          1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dnr.8200",     0,  0,0x14, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dnr.8500",     1,  0,0x15, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dnrv",         1,  0,0x15, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dnrv.8200",    0,  0,0x14, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dnrv.8500",    1,  0,0x15, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dv",           1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dv.8200",      0,  0,0x14, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dv.8500",      1,  0,0x15, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
        { 0,},
    }},
    {"IRIX+5", "EXB-82","JAG",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, Exabyte_density_trans,
	"/dev/rmt/jag%dd%d","/dev/rmt/jag%dd%d", 2, IRIXfindVME, {
    /*   string                          den mod hwd pas fxd rewind         sf,1st */
    /*   ======                          === === === === === ======         === */
	/* Default, passthru */
	{ "/dev/rmt/jag%dd%dnrv",         0,  0,   0, 0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/scsi/jag%dd%dl0",        -1,  0,  -1, 1,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	/* Usable variable */
	{ "/dev/rmt/jag%dd%dnrv",         0,  0,   0, 0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	/* Usable Fixed */
	{ "/dev/rmt/jag%dd%dnr",          0,  0,   0, 0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	/* Descriptive */
	{ "/dev/rmt/jag%dd%dstat",       -1,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%d",            0,  0,   0, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dv",           0,  0,   0, 0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
    {"IRIX+5", "DLT","JAG",  FTT_FLAG_CHK_BOT_AT_FMK|FTT_FLAG_BSIZE_AFTER, 
	FTT_OP_GET_STATUS, ftt_trans_table, DLT_density_trans,
	"/dev/rmt/jag%dd%d","/dev/rmt/jag%dd%d", 2, IRIXfindVME, {
    /*   string                          den mod hwd pas fxd rewind         sf,1st */
    /*   ======                          === === === === === ======         === */
	/* Default, passthru */
	{ "/dev/rmt/jag%dd%dnrv",         5,  0, 0x1A,0,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/scsi/jag%dd%dl0",        -1,  0,  -1, 1,  0,                 0, 1, IRIX_MAX_BLKSIZE},
	/* Usable variable */
	{ "/dev/rmt/jag%dd%dnrv",         5,  0, 0x1A,0,  0,                 0, 0, IRIX_MAX_BLKSIZE},
	/* Usable Fixed */
	{ "/dev/rmt/jag%dd%dnr",          5,  0, 0x1A,0,  1,                 0, 1, IRIX_MAX_BLKSIZE},
	/* Descriptive */
	{ "/dev/rmt/jag%dd%dstat",       -1,  0,  -1, 0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%d",            5,  0, 0x1A,0,  1,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ "/dev/rmt/jag%dd%dv",           5,  0, 0x1A,0,  0,          FTT_RWOC, 1, IRIX_MAX_BLKSIZE},
	{ 0,},
    }},
    {"", "", "unknown", FTT_FLAG_REOPEN_AT_EOF, 0, ftt_trans_table, 
	Generic_density_trans, "%3$s", "%3$s", 1, "echo", {
	/*   string   den mod hwd    pas fxd rewind 1st */
	/*   ======   === === ===    === === ====== === */
	{ "%3$s",      0,  0,  0,  0,  0,  0,     1, EXB_MAX_BLKSIZE},
	{ 0,},
    }},
    {0, },
};


ftt_stat_entry ftt_stat_op_tab[] = {
    {"EXB-8200", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|FTT_DO_EXBRS},

    {"EXB-8500", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|
	FTT_DO_EXBRS|FTT_DO_SN|FTT_DO_RP_SOMETIMES},

    {"EXB-8505", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px20_EXB|FTT_DO_RS|
	FTT_DO_EXBRS| FTT_DO_05RS|FTT_DO_SN|FTT_DO_LSRW|
	FTT_DO_RP_SOMETIMES},

    {"EXB-8900", 
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_RS|
	FTT_DO_EXBRS|FTT_DO_05RS|FTT_DO_MS_Px10|FTT_DO_SN|
	FTT_DO_LSRW|FTT_DO_RP_SOMETIMES},

    {"DLT",      
	FTT_DO_TUR|FTT_DO_INQ|FTT_DO_MS|FTT_DO_MS_Px10|FTT_DO_RS|
	FTT_DO_DLTRS|FTT_DO_SN|FTT_DO_LSRW|FTT_DO_LSC|FTT_DO_RP},

    {0,0}
};
