/* 
** tape operation includes and definitions.
** since these all derive from mtio, they 
** are all of the form
** struct tapeop { int tape_op, tape_count;} buf;
** ioctl(fd, FTT_TAPE_OP, &buf);
** with various operations.  We include and define here
** so that we get useful definitions all around.
*/
#include <sys/buf.h>
#include <sys/device.h>
#include <sys/devinfo.h>
#include <sys/ioctl.h>
#include <sys/scsi.h>
#include <sys/tape.h>
#define tapeop stop
#define FTT_TAPE_NOP   STFSR
#define FTT_TAPE_RETEN STRETEN
#define FTT_TAPE_ERASE STERASE
#define FTT_TAPE_REW STREW
#define FTT_TAPE_RSF STRSF
#define FTT_TAPE_FSF STFSF
#define FTT_TAPE_RSR STRSR
#define FTT_TAPE_FSR STFSR
#define FTT_TAPE_WEOF STWEOF
/* pre 3.2 doesn't have offline */
#ifdef STOFFL
#define FTT_TAPE_UNLOAD STOFFL
#else
#define FTT_TAPE_UNLOAD -1
#endif
#define	FTT_TAPE_OP STIOCTOP
#define tape_op st_op
#define tape_count st_count
