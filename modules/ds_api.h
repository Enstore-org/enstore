#define CLEAN 3
#define REPAIR 4
#define INSTALL 5

#define DELTA 1
#define SUM_OF_DELTAS 2
#define BUMP 4

#include "ftt.h" 

typedef struct ds_open_parms {
  char drive_serial_number[MAX_DRIVE_SERIAL_NUMBER_LEN + 1];
  char vendor[MAX_VENDOR_LEN + 1];
  char product_type[MAX_PRODUCT_TYPE_LEN + 1];
  ftt_descriptor ftt_d;
} DS_OPEN_PARMS;

typedef struct ds_stats {
  int power_hrs;
  int motion_hrs;
  int mb_user_read;
  int mb_user_write;
  int mb_dev_read;
  int mb_dev_write;
  int read_errors;
  int write_errors;
  int track_retries;
  int underrun;
  int mount_count;
} DS_STATS;

typedef struct ds_descriptor {
  int  init_flag;
  int  delta_flag;
  int  delta_sum_flag;
  char drive_serial_number[MAX_DRIVE_SERIAL_NUMBER_LEN + 1];
  char vendor[MAX_VENDOR_LEN + 1];
  char product_type[MAX_PRODUCT_TYPE_LEN + 1];
  char operation[MAX_OPERATION_SIZE + 1];
  int cleaning_bit;
 
  char tape_volser[MAX_VOLSER_LEN + 1];
  
  ftt_descriptor ftt_d;
  DS_STATS init_stats;
  DS_STATS delta;
  DS_STATS sum_of_deltas;
  char host[MAX_HOST_LEN + 1];
  char logical_drive_name[MAX_LOGICAL_DRIVE_NAME_LEN + 1];
} DS_DESCRIPTOR;

typedef struct ds_report {
  char drive_serial_number[MAX_DRIVE_SERIAL_NUMBER_LEN + 1];
  char vendor[MAX_VENDOR_LEN + 1];
  char product_type[MAX_PRODUCT_TYPE_LEN + 1];
  char timestamp[MAX_DATE_LEN + 1];
  char tape_volser[MAX_VOLSER_LEN + 1];
  char operation[MAX_OPERATION_SIZE + 1];
  int power_hrs;
  int motion_hrs;
  int cleaning_bit;
  int mb_user_read;
  int mb_user_write;
  int mb_dev_read;
  int mb_dev_write;
  int read_errs;
  int write_errs;
  int track_retries;
  int underrun;
} DS_REPORT;

  
