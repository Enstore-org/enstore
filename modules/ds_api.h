/*static char *id="$Id$" */
#ifndef _DS_API_H
#define _DS_API_H

#include "drivestat.h"
#include "ftt.h"

#include <time.h>

#define CLEAN  3
#define REPAIR 4
#define INSTALL 5

#define DELTA 1
#define SUM_OF_DELTAS 2
#define BUMP_MOUNTS 4
#define ABSOLUTE 8
#define INIT 16
#define RECENT 32

#define DRIVE_SERIAL_NUMBER 1
#define VENDOR 2
#define PRODUCT_TYPE 3
#define OPERATION 4
#define TAPE_VOLSER 5
#define HOST 6
#define LOGICAL_DRIVE_NAME 7

#define FTT_DESC 1 

time_t time_stamp;

/* typedef struct ds_open_parms { */
/*   char drive_serial_number[MAX_DRIVE_SERIAL_NUMBER_LEN + 1]; */
/*   char vendor[MAX_VENDOR_LEN + 1]; */
/*   char product_type[MAX_PRODUCT_TYPE_LEN + 1]; */
/*   ftt_descriptor ftt_d; */
/* } DS_OPEN_PARMS; */

typedef struct ds_stats {
  int init_flag;
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
  int  ds_init_flag;
#
  char drive_serial_number[MAX_DRIVE_SERIAL_NUMBER_LEN + 1];
  char vendor[MAX_VENDOR_LEN + 1];
  char product_type[MAX_PRODUCT_TYPE_LEN + 1];
#
  char logical_drive_name[MAX_LOGICAL_DRIVE_NAME_LEN + 1];

  char host[MAX_HOST_LEN + 1];
#
  char tape_volser[MAX_VOLSER_LEN + 1];
#
  char operation[MAX_OPERATION_SIZE + 1];
  int  cleaning_bit;
#
  ftt_descriptor ftt_d;
#
  DS_STATS init_stats;
  DS_STATS delta;
  DS_STATS sum_of_deltas;
  DS_STATS recent_stats;

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

/*
 * Prototypes
 */

DS_DESCRIPTOR *ds_alloc();
int ds_translate_ftt_drive_id(DS_DESCRIPTOR* const ds_desc,const ftt_stat_buf ftt_stat_buff);
int ds_translate_ftt_stats(DS_DESCRIPTOR* const ds_desc,const ftt_stat_buf ftt_stat_buff,const int flag);
void ds_free(DS_DESCRIPTOR* ds_desc);
int ds_init(DS_DESCRIPTOR* ds_desc, const ftt_descriptor ftt_d);
int ds_update(DS_DESCRIPTOR* ds_desc, const ftt_descriptor ftt_d);
int ds_print(const DS_DESCRIPTOR* const ds_desc, const char* const file);
void ds_print_stats(FILE* const fp,const char* const stat_name, const DS_STATS* const stats_pointer);
int ds_set_character_field(DS_DESCRIPTOR* ds_desc,const char* const string, const int field);
int ds_set_stats(DS_DESCRIPTOR* const ds_desc, const DS_STATS* const stat_buf, const int flag);
int ds_extract_stats(DS_DESCRIPTOR* const ds_desc, DS_STATS* const stat_buf, const int flag);
int ds_bump_deltasum(DS_DESCRIPTOR* const ds_desc, const DS_STATS* const stat_buf);
int ds_compute_delta(DS_DESCRIPTOR* ds_desc);
int ds_send_stats(const DS_DESCRIPTOR* const ds_desc, const int timeout,
		  const int flag);
int ds_drive_maintenance(const DS_DESCRIPTOR* const ds_desc,const int flag,
			 const char* const host,
			 const char* const logical_drive_name);

int ds_prepare_list(const char* const drive,const char* const vendor,
		    const char* const prod_type,
		    const char* const host, const char* const vsn,
		    const char* const bdate,
		    const char* const edate,int *n);
DS_REPORT *ds_getnext(int list_sd);
int ds_close_list(int list_sd);

#endif
