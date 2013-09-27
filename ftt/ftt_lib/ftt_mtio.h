/* 
** tape operation includes and definitions.
** since these all derive from mtio, they 
** are all of the form
** struct tapeop { int tape_op, tape_count;} buf;
** ioctl(fd, TAPE_OP, &buf);
** with various operations.  We include and define here
** so that we get useful definitions all around.
*/

#ifdef SunOS
#include <sys/types.h>
#include <sys/errno.h>
#include <sys/ioctl.h>
#include <sys/signal.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mtio.h>
#define tapeop mtop
#define TAPE_NOP   MTNOP
#define TAPE_RETEN MTRETEN
#define TAPE_ERASE MTERASE
#define TAPE_REW MTREW
#define TAPE_RSF MTBSF
#define TAPE_RSR MTBSR
#define TAPE_FSF MTFSF
#define TAPE_FSR MTFSR
#define TAPE_WEOF MTWEOF
#define TAPE_UNLOAD MTOFFL
#define TAPE_OP MTIOCTOP
#define tape_op mt_op
#define tape_count mt_count
#endif

#ifdef IRIX
#include <sys/types.h>
#include <sys/mtio.h>
#define tapeop mtop
#define TAPE_NOP  MTNOP
#define TAPE_RETEN MTRET
#define TAPE_ERASE MTERASE
#define TAPE_REW MTREW
#define TAPE_RSF MTBSF
#define TAPE_RSR MTBSR
#define TAPE_FSF MTFSF
#define TAPE_FSR MTFSR
#define TAPE_WEOF MTWEOF
#define TAPE_UNLOAD MTUNLOAD
#define TAPE_OP MTIOCTOP
#define tape_op mt_op
#define tape_count mt_count
#endif

#ifdef AIX
#include <sys/buf.h>
#include <sys/device.h>
#include <sys/devinfo.h>
#include <sys/ioctl.h>
#include <sys/scsi.h>
#include <sys/tape.h>
#define tapeop stop
#define TAPE_NOP   STFSR
#define TAPE_RETEN STRETEN
#define TAPE_ERASE STERASE
#define TAPE_REW STREW
#define TAPE_RSF STRSF
#define TAPE_RSR STRSR
#define TAPE_FSR STFSR
#define TAPE_FSF STFSF
#define TAPE_WEOF STWEOF
/* pre 3.2 doesn't have offline */
#ifdef STOFFL
#define TAPE_UNLOAD STOFFL
#else
#define TAPD_UNLOAD -1
#endif
#define	TAPE_OP STIOCTOP
#define tape_op st_op
#define tape_count st_count
#endif
