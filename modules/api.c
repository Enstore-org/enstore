/*static char *rcsid="$Id$" */
/**************************************************************************
 *                                                                        *
 * api.c                                                                  *
 *    API library for drivestat.                                          *
 *                                                                        *
 **************************************************************************/

#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h> 


#include <sys/socket.h> 
#include <netinet/in.h> 
#include <arpa/inet.h> 
#include <netdb.h>

#include <time.h>
#include "drivestat.h"
#include "ds_api.h"
#include "ftt.h"

/*
 * Prototypes
 */

DS_DESCRIPTOR *ds_open(DS_OPEN_PARMS *parms, int flag);
DS_REPORT *ds_getnext(int list_sd);
int ds_print_stats(DS_DESCRIPTOR * d,char *file);

/*************************************************************************
 *                                                                       *
 * ds_open()                                                             *
 *   Create a drivestat descriptor.                                      *
 *                                                                       *
 *************************************************************************/

DS_DESCRIPTOR *ds_open(DS_OPEN_PARMS *parms, int flag)
{
  DS_DESCRIPTOR *d;
  ftt_stat_buf stat_buff;
  int status;
  char *value;
  

  d = (DS_DESCRIPTOR *) malloc(sizeof(DS_DESCRIPTOR));

  if (d == NULL)
    return (NULL);

/*
 * If an ftt descriptor is being used, call ftt to get the drive_serial_number
 * vendor, and product_type
 */

  
  if (flag == FTT_DESC) {   
    d->ftt_d = parms->ftt_d;
    stat_buff = ftt_alloc_stat();
    status = ftt_get_stats(d->ftt_d,stat_buff);
    if (status != FTT_SUCCESS)
       return(NULL);
    
    value = ftt_extract_stats(stat_buff,FTT_SERIAL_NUM);
    if (value)
       strcpy(d->drive_serial_number,value);
    else
       return (NULL);

    value = ftt_extract_stats(stat_buff,FTT_VENDOR_ID);
    if (value)
       strcpy(d->vendor,value);
    else
       return (NULL);
    
    value = ftt_extract_stats(stat_buff,FTT_PRODUCT_ID);
    if (value)
       strcpy(d->product_type,value);
    else
       return (NULL);

  } 
  else  {   /* need to get the device type based on this */
    strcpy(d->drive_serial_number,parms->drive_serial_number);
    strcpy(d->vendor,parms->vendor);
    strcpy(d->product_type,parms->product_type);
  }

  if ( gethostname(d->host,MAX_HOST_LEN) != 0) 
      strcpy(d->host,"UNKNOWN");
  
  d->init_flag = 0;
  d->delta_flag = 0;
  d->delta_sum_flag = 0;
  strcpy(d->tape_volser,"UNKNOWN");
  strcpy(d->operation,"UNKNOWN");

  d->init_stats.power_hrs = 0;
  d->init_stats.motion_hrs = 0;
  d->init_stats.read_errors = 0;
  d->init_stats.write_errors = 0;
  d->init_stats.mb_user_read = 0;
  d->init_stats.mb_user_write = 0;
  d->init_stats.mb_dev_read = 0;
  d->init_stats.mb_dev_write = 0;
  d->init_stats.track_retries = 0;
  d->init_stats.underrun = 0;
  d->init_stats.mount_count = 0;
  
  d->delta.power_hrs = 0;
  d->delta.motion_hrs = 0;
  d->delta.read_errors = 0;
  d->delta.write_errors = 0;
  d->delta.mb_user_read = 0;
  d->delta.mb_user_write = 0;
  d->delta.mb_dev_read = 0;
  d->delta.mb_dev_write = 0;
  d->delta.track_retries = 0;
  d->delta.underrun = 0;
  d->delta.mount_count = 0;

  d->sum_of_deltas.power_hrs = 0;
  d->sum_of_deltas.motion_hrs = 0;
  d->sum_of_deltas.read_errors = 0;
  d->sum_of_deltas.write_errors = 0;
  d->sum_of_deltas.mb_user_read = 0;
  d->sum_of_deltas.mb_user_write = 0;
  d->sum_of_deltas.mb_dev_read = 0;
  d->sum_of_deltas.mb_dev_write = 0;
  d->sum_of_deltas.track_retries = 0;
  d->sum_of_deltas.underrun = 0;
  d->sum_of_deltas.mount_count = 0;

  return d;
}

/****************************************************************************
 *                                                                          *
 * ds_init_stats()                                                          *
 *   Make a call to ftt and get the initial statistics to be used to compute*
 *   the deltas.                                                            *
 *                                                                          *
 ****************************************************************************/
int ds_init_stats(DS_DESCRIPTOR *d)
{
  ftt_stat_buf stat_buff;
  int status;
  char *value;
  float float_value;

  
  stat_buff = ftt_alloc_stat();
  status = ftt_get_stats(d->ftt_d,stat_buff);
  if (status != FTT_SUCCESS)
    return(-1);


  /*
   * Power Hours
   */

  value = ftt_extract_stats(stat_buff,FTT_POWER_HOURS);
  if (value)
     d->init_stats.power_hrs = atoi(value);
  else
     d->init_stats.power_hrs = -1;
    

  /*
   * Motion Hours
   */

  value = ftt_extract_stats(stat_buff,FTT_MOTION_HOURS);
  if (value)
     d->init_stats.motion_hrs = atoi(value);
  else
     d->init_stats.motion_hrs = -1;


  /*
   * MB_USER_READ
   */

  value = ftt_extract_stats(stat_buff,FTT_USER_READ);
  if (value) {
     float_value = (atoi(value) / 1000.0) + .5;
     d->init_stats.mb_user_read = (int) float_value;
  }
  else
     d->init_stats.mb_user_read = -1;

  /*
   * MB_USER_WRITE
   */

  value = ftt_extract_stats(stat_buff,FTT_USER_WRITE);
  if (value) {
     float_value = (atoi(value) / 1000.0) + .5;
     d->init_stats.mb_user_write = (int) float_value;
  }
  else
     d->init_stats.mb_user_write = -1;

  /*
   * MB_DEV_READ
   */
    
  value = ftt_extract_stats(stat_buff,FTT_READ_COUNT);
  if (value) {
      float_value = (atoi(value) / 1000.0) + .5;
      d->init_stats.mb_dev_read = (int) float_value;
  }
  else
      d->init_stats.mb_dev_read = -1;

  /*
   * MB_DEV_WRITE
   */
    
  value = ftt_extract_stats(stat_buff,FTT_WRITE_COUNT);
  if (value) {
     float_value = (atoi(value) / 1000.0) + .5;
     d->init_stats.mb_dev_write = (int) float_value;
  }
  else
     d->init_stats.mb_dev_write = -1;

  /*
   * Read Errors
   */
  
  value = ftt_extract_stats(stat_buff,FTT_READ_ERRORS);
  if (value) 
     d->init_stats.read_errors = atoi(value);
  else
     d->init_stats.read_errors = -1;
  
  /*
   * WRITE Errors
   */
  
  value = ftt_extract_stats(stat_buff,FTT_WRITE_ERRORS);
  if (value) 
     d->init_stats.write_errors = atoi(value);
  else
     d->init_stats.write_errors = -1;

  /*
   * Track Retries
   */

  value = ftt_extract_stats(stat_buff,FTT_TRACK_RETRY);
  if (value)
      d->init_stats.track_retries = atoi(value);
  else
      d->init_stats.track_retries = -1;

  /*
   * Underrun
   */

  value = ftt_extract_stats(stat_buff,FTT_UNDERRUN);
  if (value)
     d->init_stats.underrun = atoi(value);
  else
     d->init_stats.underrun = -1;

  /*
   * mount count, tape volser, etc...
   */

  d->init_flag = 1;
  d->cleaning_bit = 0;
  strcpy(d->tape_volser,"UNKNOWN");

  return(0);
}

/***************************************************************************
 *                                                                         *
 * ds_print_stats()                                                        *
 *   Print stats in current drivestat descriptor to file.  If file is NULL,*
 *   reports are printed to stdout.  Return 0 on success, 1 on failure.    *
 *                                                                         *
 ***************************************************************************/

int ds_print_stats(DS_DESCRIPTOR *d,char *file)
{
  char s[1024];
  FILE *fp;
  char *title;
  int i;


  if (file == NULL) {
    printf("HOSTNAME: %s\n",d->host);
    printf("DRIVE SERNO: %s\n",d->drive_serial_number);
    printf("VENDOR: %s\n",d->vendor);
    printf("PROD TYPE: %s\n",d->product_type);
    printf("LOGICAL NAME: %s\n",d->logical_drive_name);
    printf("VOLSER: %s\n",d->tape_volser);
    printf("OPERATION: %s\n",d->operation);
    printf("CLEANING BIT: %d\n",d->cleaning_bit);

    printf("INIT_FLAG: %d\n",d->init_flag);
    printf("DELTA_FLAG: %d\n",d->delta_flag);
    printf("DELTA_SUM_FLAG: %d\n",d->delta_sum_flag);

    printf("INIT PWR HRS: %d\n",d->init_stats.power_hrs); 
    printf("INIT MOT HRS: %d\n",d->init_stats.motion_hrs);
    printf("INIT RD ERR: %d\n",d->init_stats.read_errors);
    printf("INIT WR ERR: %d\n",d->init_stats.write_errors);
    printf("INIT MB UREAD: %d\n",d->init_stats.mb_user_read);
    printf("INIT MB UWRITE: %d\n",d->init_stats.mb_user_write);
    printf("INIT MB DREAD: %d\n",d->init_stats.mb_dev_read);
    printf("INIT MB DWRITE: %d\n",d->init_stats.mb_dev_write);
    printf("INIT RETRIES: %d\n",d->init_stats.track_retries);
    printf("INIT UNDERRUN: %d\n",d->init_stats.underrun);
    printf("INIT MOUNT CT: %d\n",d->init_stats.mount_count);
    
    printf("DELTA PWR HRS: %d\n",d->delta.power_hrs); 
    printf("DELTA MOT HRS: %d\n",d->delta.motion_hrs);
    printf("DELTA RD ERR: %d\n",d->delta.read_errors);
    printf("DELTA WR ERR: %d\n",d->delta.write_errors);
    printf("DELTA MB UREAD: %d\n",d->delta.mb_user_read);
    printf("DELTA MB UWRITE: %d\n",d->delta.mb_user_write);
    printf("DELTA MB DREAD: %d\n",d->delta.mb_dev_read);
    printf("DELTA MB DWRITE: %d\n",d->delta.mb_dev_write);
    printf("DELTA RETRIES: %d\n",d->delta.track_retries);
    printf("DELTA UNDERRUN: %d\n",d->delta.underrun);
    printf("DELTA MOUNT CT: %d\n",d->delta.mount_count);

    printf("SUM DELTA PWR HRS: %d\n",d->sum_of_deltas.power_hrs); 
    printf("SUM DELTA MOT HRS: %d\n",d->sum_of_deltas.motion_hrs);
    printf("SUM DELTA RD ERR: %d\n",d->sum_of_deltas.read_errors);
    printf("SUM DELTA WR ERR: %d\n",d->sum_of_deltas.write_errors);
    printf("SUM DELTA MB UREAD: %d\n",d->sum_of_deltas.mb_user_read);
    printf("SUM DELTA MB UWRITE: %d\n",d->sum_of_deltas.mb_user_write);
    printf("SUM DELTA MB DREAD: %d\n",d->sum_of_deltas.mb_dev_read);
    printf("SUM DELTA MB DWRITE: %d\n",d->sum_of_deltas.mb_dev_write);
    printf("SUM DELTA RETRIES: %d\n",d->sum_of_deltas.track_retries);
    printf("SUM DELTA UNDERRUN: %d\n",d->sum_of_deltas.underrun);
    printf("SUM DELTA MOUNT CT: %d\n",d->sum_of_deltas.mount_count);

    return(0);
  }
  else {
    
    if ((fp = fopen(file,"w")) == NULL)
      return(-1);
    
    if ((fprintf(fp,"HOSTNAME: %s\n",d->host)) <= 0) {
	fclose(fp);
	return(-1);
    }

    fprintf(fp,"DRIVE SERNO: %s\n",d->drive_serial_number);
    fprintf(fp,"VENDOR: %s\n",d->vendor);
    fprintf(fp,"PROD TYPE: %s\n",d->product_type);
    fprintf(fp,"LOGICAL NAME: %s\n",d->logical_drive_name);
    fprintf(fp,"VOLSER: %s\n",d->tape_volser);
    fprintf(fp,"OPERATION: %s\n",d->operation);
    fprintf(fp,"CLEANING BIT: %s\n",d->cleaning_bit);
   
    fprintf(fp,"INIT_FLAG: %d\n",d->init_flag);
    fprintf(fp,"DELTA_FLAG: %d\n",d->delta_flag);
    fprintf(fp,"DELTA_SUM_FLAG: %d\n",d->delta_sum_flag);

    fprintf(fp,"INIT PWR HRS: %d\n",d->init_stats.power_hrs); 
    fprintf(fp,"INIT MOT HRS: %d\n",d->init_stats.motion_hrs);
    fprintf(fp,"INIT RD ERR: %d\n",d->init_stats.read_errors);
    fprintf(fp,"INIT WR ERR: %d\n",d->init_stats.write_errors);
    fprintf(fp,"INIT MB UREAD: %d\n",d->init_stats.mb_user_read);
    fprintf(fp,"INIT MB UWRITE: %d\n",d->init_stats.mb_user_write);
    fprintf(fp,"INIT MB DREAD: %d\n",d->init_stats.mb_dev_read);
    fprintf(fp,"INIT MB DWRITE: %d\n",d->init_stats.mb_dev_write);
    fprintf(fp,"INIT RETRIES: %d\n",d->init_stats.track_retries);
    fprintf(fp,"INIT UNDERRUN: %d\n",d->init_stats.underrun);
    fprintf(fp,"INIT MOUNT CT: %d\n",d->init_stats.mount_count);
    
    fprintf(fp,"DELTA PWR HRS: %d\n",d->delta.power_hrs); 
    fprintf(fp,"DELTA MOT HRS: %d\n",d->delta.motion_hrs);
    fprintf(fp,"DELTA RD ERR: %d\n",d->delta.read_errors);
    fprintf(fp,"DELTA WR ERR: %d\n",d->delta.write_errors);
    fprintf(fp,"DELTA MB UREAD: %d\n",d->delta.mb_user_read);
    fprintf(fp,"DELTA MB UWRITE: %d\n",d->delta.mb_user_write);
    fprintf(fp,"DELTA MB DREAD: %d\n",d->delta.mb_dev_read);
    fprintf(fp,"DELTA MB DWRITE: %d\n",d->delta.mb_dev_write);
    fprintf(fp,"DELTA RETRIES: %d\n",d->delta.track_retries);
    fprintf(fp,"DELTA UNDERRUN: %d\n",d->delta.underrun);
   
    fprintf(fp,"SUM DELTA PWR HRS: %d\n",d->sum_of_deltas.power_hrs); 
    fprintf(fp,"SUM DELTA MOT HRS: %d\n",d->sum_of_deltas.motion_hrs);
    fprintf(fp,"SUM DELTA RD ERR: %d\n",d->sum_of_deltas.read_errors);
    fprintf(fp,"SUM DELTA WR ERR: %d\n",d->sum_of_deltas.write_errors);
    fprintf(fp,"SUM DELTA MB UREAD: %d\n",d->sum_of_deltas.mb_user_read);
    fprintf(fp,"SUM DELTA MB UWRITE: %d\n",d->sum_of_deltas.mb_user_write);
    fprintf(fp,"SUM DELTA MB DREAD: %d\n",d->sum_of_deltas.mb_dev_read);
    fprintf(fp,"SUM DELTA MB DWRITE: %d\n",d->sum_of_deltas.mb_dev_write);
    fprintf(fp,"SUM DELTA RETRIES: %d\n",d->sum_of_deltas.track_retries);
    fprintf(fp,"SUM DELTA UNDERRUN: %d\n",d->sum_of_deltas.underrun);
    fprintf(fp,"SUM DELTA MOUNT CT: %d\n",d->sum_of_deltas.mount_count);
 
    fclose(fp);
    return(0);
  }
}

/*************************************************************************
 *                                                                       *
 * ds_set_delta()                                                        *
 *    Set the delta portion of the ds_descriptor with the data passed in *
 *    in the stat_buf.                                                   *
 *                                                                       *
 *************************************************************************/

int ds_set_delta(DS_DESCRIPTOR *d, DS_STATS *stat_buf)
{
  d->delta.power_hrs = stat_buf->power_hrs;
  d->delta.motion_hrs = stat_buf->motion_hrs;
  d->delta.read_errors = stat_buf->read_errors;
  d->delta.mb_user_read = stat_buf->mb_user_read;
  d->delta.mb_user_write = stat_buf->mb_user_write;
  d->delta.mb_dev_read = stat_buf->mb_dev_read;
  d->delta.mb_dev_write = stat_buf->mb_dev_write;
  d->delta.write_errors = stat_buf->write_errors;
  d->delta.track_retries = stat_buf->track_retries;
  d->delta.underrun = stat_buf->underrun;
  d->delta.mount_count = stat_buf->mount_count;
  d->delta_flag = 1;
  return (0);
}

/*************************************************************************
 *                                                                       *
 * ds_set_deltasum()                                                     *
 *    Set the delta sum portion of the ds_descriptor with the data passed*
 *    in the stat_buf.                                                   *
 *                                                                       *
 *************************************************************************/

int ds_set_deltasum(DS_DESCRIPTOR *d, DS_STATS *stat_buf)
{
  d->sum_of_deltas.power_hrs = stat_buf->power_hrs;
  d->sum_of_deltas.motion_hrs = stat_buf->motion_hrs;
  d->sum_of_deltas.read_errors = stat_buf->read_errors;
  d->sum_of_deltas.mb_user_read = stat_buf->mb_user_read;
  d->sum_of_deltas.mb_user_write = stat_buf->mb_user_write;
  d->sum_of_deltas.mb_dev_read = stat_buf->mb_dev_read;
  d->sum_of_deltas.mb_dev_write = stat_buf->mb_dev_write;
  d->sum_of_deltas.write_errors = stat_buf->write_errors;
  d->sum_of_deltas.track_retries = stat_buf->track_retries;
  d->sum_of_deltas.underrun = stat_buf->underrun;
  d->sum_of_deltas.mount_count = stat_buf->mount_count;
  d->delta_sum_flag = 1;
  return (0);
}

/*************************************************************************
 *                                                                       *
 * ds_set_init()                                                         *
 *    Set the init portion of the ds_descriptor with the data passed in  *
 *    in the stat_buf.                                                   *
 *                                                                       *
 *************************************************************************/


int ds_set_init(DS_DESCRIPTOR *d, DS_STATS *stat_buf)
{
  d->init_stats.power_hrs = stat_buf->power_hrs;
  d->init_stats.motion_hrs = stat_buf->motion_hrs;
  d->init_stats.read_errors = stat_buf->read_errors;
  d->init_stats.write_errors = stat_buf->write_errors;
  d->init_stats.mb_user_read = stat_buf->mb_user_read;
  d->init_stats.mb_user_write = stat_buf->mb_user_write;
  d->init_stats.mb_dev_read = stat_buf->mb_dev_read;
  d->init_stats.mb_dev_write = stat_buf->mb_dev_write;

  d->init_stats.track_retries = stat_buf->track_retries;
  d->init_stats.underrun = stat_buf->underrun;
  d->init_stats.mount_count = stat_buf->mount_count;
  d->init_flag = 1;
  return 0;
}

/************************************************************************
 *                                                                      *
 * ds_set_operation()                                                   *
 *   Set the operation field in the drivestat descriptor.               *
 *                                                                      *
 ************************************************************************/

int ds_set_operation(DS_DESCRIPTOR *d,char *s)
{
  strcpy(d->operation,s);
  return(0);
}


/************************************************************************
 *                                                                      *
 * ds_compute_delta                                                     *
 *   Gathers the stats from the tape drive, and computes the delta based*
 *   on the current stats and the stats in the init portion of the      *
 *   descriptor.                                                        *
 *                                                                      *
 ************************************************************************/

int ds_compute_delta(DS_DESCRIPTOR *d)
{
  DS_STATS current_stats;
  int rc;

  d->delta_flag = 1;
  
  rc = i_get_current_drive_stats(&current_stats,d);
  if (rc)
    return(-1);
  
  d->delta.power_hrs = current_stats.power_hrs - d->init_stats.power_hrs;
  d->delta.motion_hrs = current_stats.motion_hrs - d->init_stats.motion_hrs;
  d->delta.mb_user_read = current_stats.mb_user_read - d->init_stats.mb_user_read;
  d->delta.mb_user_write = current_stats.mb_user_write - d->init_stats.mb_user_write;
  d->delta.mb_dev_read = current_stats.mb_dev_read - d->init_stats.mb_dev_read;
  d->delta.mb_dev_write= current_stats.mb_dev_write - d->init_stats.mb_dev_write;
  d->delta.read_errors = current_stats.read_errors - d->init_stats.read_errors;
  d->delta.write_errors = current_stats.write_errors - d->init_stats.write_errors;
  d->delta.track_retries = current_stats.track_retries - d->init_stats.track_retries;
  d->delta.underrun = current_stats.underrun - d->init_stats.underrun;
  d->delta.mount_count = current_stats.mount_count - d->init_stats.mount_count;
  
  return 0;

}


/************************************************************************
 *                                                                      *
 * ds_prepare_list()                                                    *
 *   Sets up a list to be fetched using the getnext function.  A socket *
 *   is established with a child of the server, and passed back through *
 *   the pointer cursor_sd.  This is used in subsequent getnext()       *
 *   calls.  The number of records that will be retrieved is returned.  *
 *                                                                      *
 ************************************************************************/

int ds_prepare_list(char *drive,char *vendor,char *prod_type,
		    char *host, char *vsn,char *bdate,
		    char *edate,int *n)
{
  char msg[4096];
  char send_buff[4096];
  char ack_msg[512];
  int i,rc,list_sd;
  int nread;

  if (drive == NULL)
    strcpy(msg,"any");
  else
    strcpy(msg,drive);
  strcat(msg,"|");

  if (vendor == NULL)
    strcat(msg,"any");
  else
    strcat(msg,vendor);
  strcat(msg,"|");
  
  if (prod_type == NULL)
    strcat(msg,"any");
  else
    strcat(msg,prod_type);
  strcat(msg,"|");
  
  if (host == NULL)
    strcat(msg,"any");
  else
    strcat(msg,prod_type);
  strcat(msg,"|");
  
  if (vsn == NULL)
    strcat(msg,"any");
  else
    strcat(msg,vsn);
  strcat(msg,"|");

  if (bdate == NULL)
    strcat(msg,"any");
  else
    strcat(msg,bdate);
  strcat(msg,"|");

  if (edate == NULL)
    strcat(msg,"any");
  else
    strcat(msg,edate);
  sprintf(send_buff,"%d|%s|%s", 
	  strlen(msg),LIST_STR,msg);
  list_sd = connect_to_server();
  rc = send_data(list_sd,send_buff);
  if (rc < 0) {
      printf("ERROR: send_data status =%d\n",list_sd);
      return(-1);
  }

  /*
   * Read the ack
   */

  *n = get_ack(list_sd);

  return(list_sd);
    
}
				
/************************************************************************
 *                                                                      *
 * ds_getnext()                                                         *
 *  Fetches the next record from the list prepared by ds_prepare_list().*
 *                                                                      *
 ************************************************************************/

DS_REPORT *ds_getnext(int list_sd)
{
  
  char msg[4096],
    *token;
  DS_REPORT *report;
  char c, incoming_len[32],*buff,*ptr;
  int rc,i,n,nread;

  /*
   * Send the getnext request to the server
   */
  printf("sending request....\n");
  sprintf(msg,"%d|%s|",0,GETNEXT_STR);
  rc = send_data(list_sd,msg);
  if (rc < 0) 
    return(NULL);

  /*
   * Get the length of the record to be retrieved.
   */
 
  printf("request sent...\n");
  rc = read(list_sd,&c,1);
  if (rc < 0)
    return(NULL);
  i = 0;
  while (c != '|') {
    incoming_len[i++] = c;
    rc = read(list_sd,&c,1);
    if (rc <0)
      return (NULL);
  }
  incoming_len[i] = '\0';
  /* 
   * Read incoming_len bytes
   */

  
  n = atoi(incoming_len);
  printf("read len = %d\n",n);
  buff = (char *) malloc(n + 1);

  if (buff == NULL) {
    return(NULL);
  }

  ptr = buff;
  while (n > 0) {
    nread = read(list_sd,ptr,n);
    if (nread > 0) {
      n -= nread;
      ptr += nread;
    }
    else {
      return(NULL);
    }
  }
  *ptr = '\0';

  /*
   * Parse the message, and put it into the DS_REPORT structure
   */
 
  printf("parsing...\n"); 
  report = (DS_REPORT *) malloc(sizeof(DS_REPORT));

  if (report == NULL)
    return(NULL);

  /*
   * Drive Serial Number
   */

  token = strtok(buff,DELIMITER);
  if (token)
    strcpy(report->drive_serial_number,token);
  else
    return(NULL);

  /*
   * Vendor
   */

  token = strtok(NULL,DELIMITER);
  if (token)
    strcpy(report->vendor,token);
  else
    return(NULL);

  /*
   * Product Type
   */

  token = strtok(NULL,DELIMITER);
  if (token)
    strcpy(report->product_type,token);
  else
    return(NULL);

  /*
   * Timestamp
   */

  token = strtok(NULL,DELIMITER);
  if (token)
    strcpy(report->timestamp,token);
  else
    return(NULL);

  /*
   * Tape volser
   */

  token = strtok(NULL,DELIMITER);
  if (token)
    strcpy(report->tape_volser,token);
  else
    return(NULL);

  /*
   * Operation
   */

  token = strtok(NULL,DELIMITER);
  if (token)
    strcpy(report->operation,token);
  else
    return(NULL);
   
  /*
   * Power Hours
   */

  token = strtok(NULL,DELIMITER);
  if (token)
    strcpy(report->operation,token);
  else
    return(NULL);
  
  /*
   * Motion Hours
   */
  
  token = strtok(NULL,DELIMITER);
  if (token)
    report->motion_hrs = atoi(token);
  else
    return(NULL);
  
  /*
   * Cleaning bit
   */
  
  token = strtok(NULL,DELIMITER);
  if (token)
    report->cleaning_bit = atoi(token);
  else
    return(NULL);

  /*
   * MB user read
   */
  
  token = strtok(NULL,DELIMITER);
  if (token)
    report->mb_user_read = atoi(token);
  else
    return(NULL);

  /*
   * MB user write
   */
  
  token = strtok(NULL,DELIMITER);
  if (token)
    report->mb_user_write = atoi(token);
  else
    return(NULL);

  /*
   * MB dev read
   */
  
  token = strtok(NULL,DELIMITER);
  if (token)
    report->mb_dev_read = atoi(token);
  else
    return(NULL);

  /*
   * MB dev write
   */
  
  token = strtok(NULL,DELIMITER);
  if (token)
    report->mb_dev_write = atoi(token);
  else
    return(NULL);


  /*
   * read errors
   */
  
  token = strtok(NULL,DELIMITER);
  if (token)
    report->read_errs = atoi(token);
  else
    return(NULL);

  /*
   * write errors
   */
  
  token = strtok(NULL,DELIMITER);
  if (token)
    report->write_errs = atoi(token);
  else
    return(NULL);

  /*
   * track retries
   */
  
  token = strtok(NULL,DELIMITER);
  if (token)
    report->track_retries = atoi(token);
  else
    return(NULL);

  /*
   * underrun
   */
  
  token = strtok(NULL,DELIMITER);
  if (token)
    report->underrun = atoi(token);
  else
    return(NULL);

  free(buff);
  return(report);
    
}

/************************************************************************
 *                                                                      *
 * ds_close_list()                                                      *
 *    Shuts down the connection to the list processing process of the   *
 *    server.                                                           *
 *                                                                      *
 ************************************************************************/  
int ds_close_list(int list_sd)
{
  char msg[64],
    *ptr;
  int rc,
    nleft,nwritten;

  strcpy(msg,CLOSE_STR);
  sprintf(msg,"%d|%s|",0,CLOSE_STR);

  rc = send_data(list_sd,msg); 
  if (rc < 0)
    return(-1);
  rc = get_ack(list_sd);
  if (rc < 0) {
     printf("ds_ack():Server reported error\n");
     return (rc);
  }

  close(list_sd);
  return(0);
}

/************************************************************************
 *                                                                      *
 * i_get_current_drive_stats                                            *
 *   internal routine to get the current drive statistics.              *
 *                                                                      *
 ************************************************************************/
int i_get_current_drive_stats(DS_STATS *stats,DS_DESCRIPTOR *d)
{
  ftt_stat_buf stat_buff;
  int status;
  char *value;
  float float_value;

  stat_buff = ftt_alloc_stat();
  status = ftt_get_stats(d->ftt_d,stat_buff);
  if (status != FTT_SUCCESS)
    return(-1);

  value = ftt_extract_stats(stat_buff,FTT_POWER_HOURS);
  if (value)
     stats->power_hrs = atoi(value);
  else
     stats->power_hrs = d->init_stats.power_hrs;

  value = ftt_extract_stats(stat_buff,FTT_MOTION_HOURS);
  if (value)
     stats->motion_hrs = atoi(value);
  else
     stats->motion_hrs = d->init_stats.motion_hrs;

  value = ftt_extract_stats(stat_buff,FTT_USER_READ);
  if (value) {
     float_value = (atoi(value) / 1000.0) + .5;
     stats->mb_user_read = (int) float_value;
  }
  else
     stats->mb_user_read = d->init_stats.mb_user_read;

  /*
   * MB_USER_WRITE
   */

  value = ftt_extract_stats(stat_buff,FTT_USER_WRITE);
  if (value) {
     float_value = (atoi(value) / 1000.0) + .5;
     stats->mb_user_write = (int) float_value;
  }
  else
     stats->mb_user_write = d->init_stats.mb_user_write;

  /*
   * MB_DEV_READ
   */
 
  value = ftt_extract_stats(stat_buff,FTT_READ_COUNT);
  if (value) {
      float_value = (atoi(value) / 1000.0) + .5;
      stats->mb_dev_read = (int) float_value;
  }
  else
      stats->mb_dev_read = d->init_stats.mb_dev_read;

  /*
   * MB_DEV_WRITE
   */
 
  value = ftt_extract_stats(stat_buff,FTT_WRITE_COUNT);
  if (value) {
     float_value = (atoi(value) / 1000.0) + .5;
     stats->mb_dev_write = (int) float_value;
  }
  else
     stats->mb_dev_write = d->init_stats.mb_dev_write;

  /*
   * Read Errors
   */
 
  value = ftt_extract_stats(stat_buff,FTT_READ_ERRORS);
  if (value)
     stats->read_errors = atoi(value);
  else
     stats->read_errors = d->init_stats.read_errors;

  /*
   * WRITE Errors
   */

  value = ftt_extract_stats(stat_buff,FTT_WRITE_ERRORS);
  if (value)
     stats->write_errors = atoi(value);
  else
     stats->write_errors = d->init_stats.write_errors;

  /*
   * Track Retries
   */

  value = ftt_extract_stats(stat_buff,FTT_TRACK_RETRY);
  if (value)
      stats->track_retries = atoi(value);
  else
      stats->track_retries = d->init_stats.track_retries;

  /*
   * Underrun
   */

  value = ftt_extract_stats(stat_buff,FTT_UNDERRUN);
  if (value)
     stats->underrun = atoi(value);
  else
     stats->underrun = d->init_stats.underrun;

  stats->mount_count = d->init_stats.mount_count;
  return(0);
}
 
/************************************************************************
 *                                                                      *
 * ds_send_stats()                                                      *
 *   Send the stats to the drivestat server.  <timeout> if non-0 will   *
 *   cause the command to block for <timeout> seconds.  <flag> instructs*
 *   what to send:                                                      *
 *      DELTA - Send the delta in the drivestat descriptor.             *
 *      SUM - Send the sum of deltas in the drivestat descriptor.       *
 *                                                                      *
 *   The format of the packet sent will be:                             *
 *     01|<DSN>|<VEN>|<PRD>|<TAPEVS>|<PHRS>|<MHRS>|<CBIT>|<USRREAD>|    *
 *     <USRWR>|<DEVREAD>|<DEVWRITE>|<RDERR>|<WRERR>|<RET>|<UND>|<MTCT>  *
 *     <HOST>|<LOGICAL_DRIVE>^                                          *
 *   Return 0 on success, -1 on error.                                  *
 *                                                                      *
 *   The ds_server port and host should be stored in the users          *
 *   environment (DS_SERVER_PORT, DS_SERVER_HOST).  This is established *
 *   if the user does a ups setup.                                      *
 *                                                                      *
 ************************************************************************/ 

int ds_send_stats(DS_DESCRIPTOR *d, int timeout, int flag)
{
  char send_buff[4096],
    msg[4096],
    s[64];

  int sd;
  int rc;

  DS_STATS stats;
   
  if (flag & DELTA) {
    if (d->delta_flag)
      stats = d->delta;
    else
      return(-1);
  }
  else {
    if (d->delta_sum_flag == 0)
      return(-2);
    stats = d->sum_of_deltas;
  }
	   
  if (strlen(d->host) == 0)
    strcpy(d->host,"UNKNOWN");

  if (strlen(d->logical_drive_name) == 0)
    strcpy(d->logical_drive_name,"UNKNOWN");

  sprintf(msg,
	  "%s|%s|%s|%s|%s|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%s|%s",
	  d->drive_serial_number,
	  d->vendor,
	  d->product_type,
	  d->tape_volser,
	  d->operation,
	  stats.power_hrs,
	  stats.motion_hrs,
	  d->cleaning_bit,
	  stats.mb_user_read,
	  stats.mb_user_write,
	  stats.mb_dev_read,
	  stats.mb_dev_write,
	  stats.read_errors,
	  stats.write_errors,
	  stats.track_retries,
	  stats.underrun,
	  stats.mount_count,
	  d->host,
	  d->logical_drive_name);

  sprintf(send_buff,"%d|%s|%s", 
	  strlen(msg),UPDATE_STR,msg);
 
  sd = connect_to_server();
  send_data(sd,send_buff);
  if (sd > 0) {
     rc = get_ack(sd);
     if (rc < 0) {
        printf("ds_send_stats():Server reported error\n");
        return (rc);
     }
     close(sd);
     return(0);
  }
  else {
    printf("send_data failed. code=%d\n",sd);
    return(sd);
  }
  printf("ds_send_stats returning\n");
}

/************************************************************************
 *                                                                      *
 * ds_drive_maintenace()                                                *
 *   Perform maintenace on the drive:                                   *
 *     INSTALL                                                          *
 *     REPAIR                                                           *
 *     CLEAN                                                            *
 *                                                                      *
 ************************************************************************/

int ds_drive_maintenance(DS_DESCRIPTOR *d,int flag,
			 char *host,char *logical_drive_name)
{

  char send_buff[4096],
      msg[4096];
  char hostn[MAX_HOST_LEN + 1];
  char ldn[MAX_LOGICAL_DRIVE_NAME_LEN + 1];
  char *hostp;
  char *ldnp;
  int sd;

  if (host != NULL) {
      hostp = host;
  } else if (d->host != NULL) {
      hostp = d->host;
  } else {
      strcpy(hostn,"UNKNOWN");
      hostp = hostn;
  }

  if (logical_drive_name != NULL) {
      ldnp = logical_drive_name;
  } else if (d->logical_drive_name != NULL) {
      ldnp = d->logical_drive_name;
  } else {
      strcpy(ldn,"UNKNOWN");
      ldnp = ldn;
  }

  if (flag == INSTALL) {
    sprintf(msg,
	    "%s|%s|%s|%s|%s",
	    d->drive_serial_number,
	    d->vendor,
	    d->product_type,
	    hostp,
	    ldnp);
    sprintf(send_buff,"%d|%s|%s^",
	    strlen(msg),INSTALL_STR,msg);
  }

  else if ((flag == REPAIR || flag == CLEAN)) {
    sprintf(msg,"%s|%s|%s",
	    d->drive_serial_number,
	    d->vendor,
	    d->product_type);
    if (flag == REPAIR)
      sprintf(send_buff,"%d|%s|%s^",
	      strlen(msg),REPAIR_STR,msg);
    else
      sprintf(send_buff,"%d|%s|%s^",
	      strlen(msg),CLEAN_STR,msg);
  }

  sd = connect_to_server();
  printf("send_buff = %s\n",send_buff);
  send_data(sd,send_buff);
  if (sd > 0) {
    close(sd);
    return(0);
  }
  else {
    printf("send_data failed. code=%d\n",sd);
    return(sd);
  }

}


/************************************************************************
 *                                                                      *
 * connect_to_server()                                                  *
 *   connect to drivestat server.                                       *
 *                                                                      *
 ************************************************************************/

int connect_to_server()
{
  struct sockaddr_in serv_addr;
  struct hostent *phe;
  char *serv_host,*serv_port;
  int serv_tcp_port;
  int sd;

  serv_host = getenv("DS_SERVER_HOST");

  if (!serv_host) {
    printf("No server_host\n");
    return(-1);
  }

  serv_port = getenv("DS_SERVER_PORT");
  if (!serv_port) {
    printf("No server port\n");
    return(-1);
  }

  serv_tcp_port = atoi(serv_port);
  bzero((char *) &serv_addr, sizeof(serv_addr));

  if (phe=(struct hostent *)gethostbyname(serv_host))
    bcopy(phe->h_addr,&serv_addr.sin_addr,phe->h_length);
  else {
    fprintf(stderr,"Name %s not in name server\n",serv_host);
    return -1;
  }

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(serv_tcp_port);
 
  if ((sd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    perror("socket");
  
  if (connect(sd, (struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) {
    printf("errno = %d\n",errno);
    perror("connect");
    return (-1);
  }
  return(sd);

}

/************************************************************************
 *                                                                      *
 * send_data()                                                          *
 *   Send the data to the server.  On success, it returns the socket    *
 *   descriptor.                                                        *
 *                                                                      *
 ************************************************************************/

int send_data(int sd,char *buf)
{
  int nleft, nwritten;
  char *ptr,
    *server_port,
    *server_host;
  int rc;
  
  rc = _write_sock(sd,buf); 
  if (rc < 0) {
    printf("Failed to write to socket. status = %d\n",rc);
    return(-1);
  }  
  else 
    return(rc);
}

/*****************************************************************************
 *                                                                           *
 * _write_sock(sd,buff)                                                      *
 *    Utility routine to write n bytes over a socket.                        *
 *                                                                           *
 *****************************************************************************/

int _write_sock(int sd,char *buf)
{
  int nwritten,nleft,n=0;
  char *ptr;

  ptr = buf;
  nleft = strlen(buf);
  while (nleft > 0) {
    if ((nwritten = write(sd,ptr,nleft)) <= 0) {
      if (errno == EINTR)
	nwritten = 0;
      else
	return (-1);
    }
    n+=nwritten;
    nleft -= nwritten;
    ptr += nwritten;
  }
  return(0);
}


int get_ack(int sd)
{ 
   int nread,
       rc,
       i;
   char ack_return_code;
   char ack_msg[256];

  /*
   * Read the ack
   */

  nread = 0;
  rc = read(sd,&ack_msg[0],1);
  nread += rc;

  if (rc < 0) {
    printf("ERROR read status =%d\n",rc);
    return -2;
  }

  if (rc == 0) {
    printf("Premature EOF encountered - did not get ACK in get_ack\n");
    sleep(20);
    return -1;
  }

  i = 0;
  while (ack_msg[i] != END_OF_MESSAGE) {
    i++;
    rc = read(sd,&ack_msg[i],1);
    nread+=rc;
    if (rc <= 0) {
	printf("ERROR read in loop status,i = %d/%d\n",rc,i);
      return -3;
    }
  }
  ack_msg[i] = '\0';
  ack_return_code = atoi(&ack_msg[4]);
  return(ack_return_code); 
}
				
