/* 
** This is dummy for Win-NT
*/
#include <sys/types.h>

typedef unsigned long	ulong;

struct tapeop {
		short mt_op;
		ulong  mt_count;
};
#define FTT_TAPE_NOP      7
#define FTT_TAPE_RETEN    3
#define FTT_TAPE_ERASE    4
#define FTT_TAPE_REW      5
#define FTT_TAPE_RSF      6
#define FTT_TAPE_RSR      8
#define FTT_TAPE_FSF      9
#define FTT_TAPE_FSR     10
#define FTT_TAPE_WEOF    11 
#define FTT_TAPE_UNLOAD  12
#define FTT_TAPE_OP      13

#define tape_op mt_op
#define tape_count mt_count
