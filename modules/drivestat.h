/*static char *id="$Id$" */
/*------------------------------------------------------------------------*
 *                                                                        *
 * drivestat.h:                                                           *
 *    Author:  J. Fromm                                                   *
 *                                                                        *
 * Include file for drivestat daemon.                                     *
 *                                                                        *
 *------------------------------------------------------------------------*/
#ifdef API
#include "ftt.h"
#endif

#define SECURE_HOSTS_TBL_SIZE 197
#define DS_SUCCESS 0
#define DS_ERROR -1
#define BAD_DATE -2
#define DRIVESTAT_CONFIG "/config/drivestat.cfg"
#define DS_PROG_NAME "ds_server"
#define CONFIG_LINE_MAX_SIZE 132
#define DELIMITER "|"
#define END_OF_MESSAGE '^'

#define PORT 1
#define ORA_USER 2
#define ORA_PW 3
#define REQUEST_DIR 4
#define DBINTER_SLEEP_INTERVAL 5
#define LOGGER_PATH 6

#define ACK_MESSAGE_SIZE 6

#define OPCODE_LENGTH 2

/*
 * opcodes
 */

#define UPDATE_STR  "01"
#define BUMP_MOUNTS_STR "02"
#define CLEAN_STR "03"
#define REPAIR_STR "04"
#define INSTALL_STR "05"
#define LIST_STR "06"
#define GETNEXT_STR "07"
#define CLOSE_STR "99"

#define OP_UPDATE 1
#define OP_BUMP_MOUNTS 2
#define OP_CLEAN 3
#define OP_REPAIR 4
#define OP_INSTALL 5
#define OP_LIST 6
#define OP_GETNEXT 7 
#define OP_CLOSE 99
#define OP_UNKNOWN 999

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
#define MAX_LOGICAL_DRIVE_NAME_LEN 80

/*
 * Log warning levels
 */

#define INFO           1
#define CAUTION        2
#define WARNING        3
#define FATAL          4

/*
 * Other Constants
 */

#define DOWN 'D'
#define UP 'U'
#define INST 'I'
#define FTT_DESC 1 

/*
 * API constants
 */


#define DRIVESTAT_BASE_REQNAME "dsRequest"
#define DRIVE_SERIAL_NUMBER "DRIVE_SERIAL_NUMBER"
#define VENDOR "VENDOR"
#define PRODUCT_TYPE "PRODUCT_TYPE"
#define HOST "HOST"
#define LOGICAL_DRIVE_NAME "LOGICAL_DRIVE_NAME"
#define TAPE_VOLSER "TAPE_VOLSER"
#define TIMESTAMP "TIMESTAMP"
#define CURRENT_WORKING_DIR "."
#define PARENT_DIR ".."


typedef struct secure_hosts_entry {
    char host[128];
    struct secure_hosts *next;
} SECURE_HOSTS_ENTRY; 


typedef struct ds_config {
  int port;
  char *oracle_username;
  char *oracle_password;
  SECURE_HOSTS_ENTRY *secure_hosts_table[SECURE_HOSTS_TBL_SIZE];
  char *request_directory;
  char *logger_path;
  int dbinter_sleep_interval;
} DS_CONFIG;

typedef struct parsed_config_line {
  int keyword;
  char *value;
} PARSED_CONFIG_LINE;



typedef struct ds_master_drivestat {
  char drive_serial_number[MAX_DRIVE_SERIAL_NUMBER_LEN + 1];
  char vendor[MAX_VENDOR_LEN + 1];
  char product_type[MAX_PRODUCT_TYPE_LEN + 1];
  
  char tape_volser[MAX_VOLSER_LEN + 1];
  char operation;
  
  int power_hours;
  int motion_hours;
  int cleaning_bit;   
  
  int mb_user_read;
  int mb_user_write;
  int mb_dev_read;
  int mb_dev_write;
  int read_errors;      /* delta */
  int write_errors;     /* delta */
  int track_retries;    /* delta */ 
  int underrun;         /* delta */
  int mount_count;
  char host[MAX_HOST_LEN + 1];   /* host name where drive is installed */
  char logical_drive_name[MAX_DRIVE_NAME_LEN + 1];
  char bdate[MAX_DATE_LEN + 1];
  char edate[MAX_DATE_LEN + 1];
} DS_MASTER_DRIVESTAT;


typedef struct ds_master_key{
  char drive_serial_number[MAX_DRIVE_SERIAL_NUMBER_LEN + 1];
  char vendor[MAX_VENDOR_LEN + 1];
  char product_type[MAX_PRODUCT_TYPE_LEN + 1];
} DS_MASTER_KEY;

typedef struct log_entry {
  char drive_serial_number[MAX_DRIVE_SERIAL_NUMBER_LEN + 1];
  char vendor[MAX_VENDOR_LEN + 1];
  char product_type[MAX_PRODUCT_TYPE_LEN + 1];
  char timestamp[MAX_DATE_LEN + 1];
  char tape_volser[MAX_VOLSER_LEN + 1];
  char operation;
  int  power_hrs;
  int  motion_hrs;
  char cleaning_bit; 
  int  mb_user_read;
  int  mb_user_write;
  int  mb_dev_read;
  int  mb_dev_write; 
  int  read_errors;
  int  write_errors;
  int  track_retries;
  int  underrun;
 } DRIVESTAT_LOG;

typedef struct request_list_struct {
  char *request;
  int seq_no;
  struct request_list_struct *next;
} REQUEST_LIST;


