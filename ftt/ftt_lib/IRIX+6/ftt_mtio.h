/* 
** tape operation includes and definitions.
** since these all derive from mtio, they 
** are all of the form
** struct tapeop { int tape_op, tape_count;} buf;
** ioctl(fd, FTT_TAPE_OP, &buf);
** with various operations.  We include and define here
** so that we get useful definitions all around.
*/
#include <sys/types.h>
#include <sys/mtio.h>
#define tapeop mtop
#define FTT_TAPE_NOP  MTNOP
#define FTT_TAPE_RETEN MTRET
#define FTT_TAPE_ERASE MTERASE
#define FTT_TAPE_REW MTREW
#define FTT_TAPE_RSF MTBSF
#define FTT_TAPE_RSR MTBSR
#define FTT_TAPE_FSF MTFSF
#define FTT_TAPE_FSR MTFSR
#define FTT_TAPE_WEOF MTWEOF
#define FTT_TAPE_UNLOAD MTUNLOAD
#define FTT_TAPE_OP MTIOCTOP
#define tape_op mt_op
#define tape_count mt_count
