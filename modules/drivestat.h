/*static char *id="$Id$" */

#ifndef _DRIVESTAT_H
#define _DRIVESTAT_H

/*------------------------------------------------------------------------*
 *                                                                        *
 * drivestat.h:                                                           *
 *    Author:  J. Fromm                                                   *
 *                                                                        *
 * Include file for drivestat daemon.                                     *
 *                                                                        *
 *------------------------------------------------------------------------*/

/*
 * opcodes
 */

#define UPDATE_STR  "01"
#define AUPDATE_STR  "11"
#define BUMP_MOUNTS_STR "02"
#define CLEAN_STR "03"
#define REPAIR_STR "04"
#define INSTALL_STR "05"
#define LIST_STR "06"
#define GETNEXT_STR "07"
#define CLOSE_STR "99"

#define OP_UPDATE 1
#define OP_AUPDATE 11
#define OP_BUMP_MOUNTS 2
#define OP_CLEAN 3
#define OP_REPAIR 4
#define OP_INSTALL 5
#define OP_LIST 6
#define OP_GETNEXT 7 
#define OP_CLOSE 99
#define OP_UNKNOWN 999

#define DELIMITER "|"
#define END_OF_MESSAGE '^'

/*
 * Length definitions
 */

#define MAX_DRIVE_SERIAL_NUMBER_LEN 20
#define MAX_VENDOR_LEN 80
#define MAX_PRODUCT_TYPE_LEN 80
#define MAX_VOLSER_LEN 20
#define MAX_HOST_LEN 80
#define MAX_DRIVE_NAME_LEN 32
#define MAX_DATE_LEN 20
#define MAX_MSG_SIZE_LEN 5
#define MAX_OPERATION_SIZE 80
#define MAX_STAT_TYPE_SIZE 80
#define MAX_LOGICAL_DRIVE_NAME_LEN 80

/*
 * Other Constants
 */

#define DOWN 'D'
#define UP 'U'
#define INST 'I'

#endif
