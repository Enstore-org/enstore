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

#include "ds_api.h"

static char *rcsid="$Id$";

void _ds_set_ds_stats(DS_STATS* const stats_pointer, const int common_value)
{

  /* function to initialise DS_STATS struct */

  stats_pointer->init_flag = common_value;
  stats_pointer->power_hrs = common_value;
  stats_pointer->motion_hrs = common_value;
  stats_pointer->read_errors = common_value;
  stats_pointer->write_errors = common_value;
  stats_pointer->mb_user_read = common_value;
  stats_pointer->mb_user_write = common_value;
  stats_pointer->mb_dev_read = common_value;
  stats_pointer->mb_dev_write = common_value;
  stats_pointer->track_retries = common_value;
  stats_pointer->underrun = common_value;
  stats_pointer->mount_count = common_value;
}

void ds_free(DS_DESCRIPTOR* ds_desc)
{
  free(ds_desc);
}

DS_DESCRIPTOR *ds_alloc() {

  DS_DESCRIPTOR* ds_desc;

  ds_desc = (DS_DESCRIPTOR* ) malloc(sizeof(DS_DESCRIPTOR));

  if (ds_desc == NULL)
    return (NULL);

  ds_desc->ds_init_flag = -1;

  ds_desc->cleaning_bit = -1;

  strcpy(ds_desc->host,"UNKNOWN");
  strcpy(ds_desc->tape_volser,"UNKNOWN");
  strcpy(ds_desc->operation,"UNKNOWN");

  strcpy(ds_desc->logical_drive_name,"UNKNOWN");

  strcpy(ds_desc->drive_serial_number,"UNKNOWN");
  strcpy(ds_desc->vendor,"UNKNOWN");
  strcpy(ds_desc->product_type,"UNKNOWN");

  _ds_set_ds_stats(&ds_desc->init_stats,-1);
  _ds_set_ds_stats(&ds_desc->delta,0);
  _ds_set_ds_stats(&ds_desc->sum_of_deltas,0);
  _ds_set_ds_stats(&ds_desc->recent_stats,-1);

  return ds_desc;

}

int ds_translate_ftt_drive_id(DS_DESCRIPTOR* const ds_desc,const ftt_stat_buf ftt_stat_buff) {

  char* value;
  int ret_code;

  if ( ds_desc->ds_init_flag == 1 ) {
    printf("ds_translate_ftt_drive_id(): Error, Allocate new descriptor first, stats not set\n");
    return (-1);
  }

  ret_code = 0;
  
  value = ftt_extract_stats(ftt_stat_buff,FTT_SERIAL_NUM);
  if (value)
    strcpy(ds_desc->drive_serial_number,value);
  else
    ret_code = -1;;
  
  value = ftt_extract_stats(ftt_stat_buff,FTT_VENDOR_ID);
  if (value)
    strcpy(ds_desc->vendor,value);
  else
    ret_code = -1;;
  
  value = ftt_extract_stats(ftt_stat_buff,FTT_PRODUCT_ID);
  if (value)
    strcpy(ds_desc->product_type,value);
  else
    ret_code = -1;;

  if ( ret_code == 0 ) {
    ds_desc->ds_init_flag = 1;
  }

  return ret_code;

} 

int ds_translate_ftt_stats(DS_DESCRIPTOR* const ds_desc,const ftt_stat_buf ftt_stat_buff,const int flag) {

  int ret_code;
  char* value;

  DS_STATS *stats_pointer;
  int *flag_pointer;

  if ( ds_desc->ds_init_flag != 1 ) {
    printf("ds_translate_ftt_stats(): Error, set drive id first, stats not set\n");
    return (-1);
  }

  if ( ( flag != INIT ) && ( flag != RECENT ) ) {
    printf("ds_translate_ftt_stats(): Error, Wrong flag, stats not set\n");
    return (-1);
  }

  if ( _ds_set_flag_pointer(ds_desc, &stats_pointer, &flag_pointer, flag) != 0 ) {
    printf("ds_translate_ftt_stats(): Error, Wrong flag, stats not set\n");
    return (-1);
  }

  if ( ( flag == INIT ) && ( *flag_pointer == 1 ) ) {
    printf("ds_translate_ftt_stats(): Error, Already initialized, will not reinitialize\n");  
    return (-1);
  }    

  _ds_set_ds_stats(stats_pointer,-1);

  ret_code = 0;

  value = ftt_extract_stats(ftt_stat_buff,FTT_POWER_HOURS);
  if (value)
    stats_pointer->power_hrs = atoi(value);
  else
    stats_pointer->power_hrs = -1;

  value = ftt_extract_stats(ftt_stat_buff,FTT_MOTION_HOURS);
  if (value)
    stats_pointer->motion_hrs = atoi(value);
  else
    stats_pointer->motion_hrs = -1;
    
  value = ftt_extract_stats(ftt_stat_buff,FTT_USER_READ);
  if (value) {
    stats_pointer->mb_user_read = (int) ( (float) atoi(value)/ 1000.0 );
  }
  else
    stats_pointer->mb_user_read = -1;

  /*
   * MB_USER_WRITE
   */

  value = ftt_extract_stats(ftt_stat_buff,FTT_USER_WRITE);
  if (value) {
     stats_pointer->mb_user_write = (int) ( (float) atoi(value)/ 1000.0 );
  }
  else
    stats_pointer->mb_user_write = -1;

  /*
   * MB_DEV_READ
   */
 
  value = ftt_extract_stats(ftt_stat_buff,FTT_READ_COUNT);
  if (value) {
      stats_pointer->mb_dev_read = (int) ( (float) atoi(value)/ 1000.0 );
  }
  else
    stats_pointer->mb_dev_read = -1;

  /*
   * MB_DEV_WRITE
   */
 
  value = ftt_extract_stats(ftt_stat_buff,FTT_WRITE_COUNT);
  if (value) {
     stats_pointer->mb_dev_write = (int) ( (float) atoi(value)/ 1000.0 );
  }
  else
    stats_pointer->mb_dev_write = -1;


  /*
   * Read Errors
   */
 
  value = ftt_extract_stats(ftt_stat_buff,FTT_READ_ERRORS);
  if (value)
     stats_pointer->read_errors = atoi(value);
  else
    stats_pointer->read_errors = -1;

  /*
   * WRITE Errors
   */

  value = ftt_extract_stats(ftt_stat_buff,FTT_WRITE_ERRORS);
  if (value)
     stats_pointer->write_errors = atoi(value);
  else
    stats_pointer->write_errors = -1;

  /*
   * Track Retries
   */

  value = ftt_extract_stats(ftt_stat_buff,FTT_TRACK_RETRY);
  if (value)
      stats_pointer->track_retries = atoi(value);
  else
    stats_pointer->track_retries = -1;

  /*
   * Underrun
   */

  value = ftt_extract_stats(ftt_stat_buff,FTT_UNDERRUN);
  if (value)
     stats_pointer->underrun = atoi(value);
  else
    stats_pointer->underrun = -1;

  /*
   * Cleaning Bit is in different area
   */

  value = ftt_extract_stats(ftt_stat_buff,FTT_CLEANING_BIT);
  if (value)
     ds_desc->cleaning_bit = atoi(value);
  else {
    ds_desc->cleaning_bit = -1;
  }

  if ( ret_code == 0 ) {
    *flag_pointer = 1;
  }

  return ret_code;

}

int ds_init(DS_DESCRIPTOR* ds_desc, const ftt_descriptor ftt_d)
/****************************************************************************
 *                                                                          *
 * ds_init()                                                          *
 *   Initialize INIT statistics using ftt; to be used to compute the deltas.     *
 *                                                                          *
 ****************************************************************************/
{
  ftt_stat_buf ftt_stat_buff;
  int status;
  int ret_code;
  float float_value;
  
  ret_code = 0;

  if ( ds_desc->init_stats.init_flag == 1 ) {
    printf("ds_init(): Error, Already initialized, will not reinitialize\n");    
    return(-1);
  }

  ftt_stat_buff = ftt_alloc_stat();
  status = ftt_get_stats(ftt_d,ftt_stat_buff);
  if ( status != FTT_SUCCESS )    {
    printf("ds_init(): Error, Could not get ftt stats\n");    
    status = ftt_free_stat(ftt_stat_buff);
    return(-1);
  }

  if ( ds_translate_ftt_drive_id(ds_desc,ftt_stat_buff)  != 0 ) {
    printf("ds_init(): Error, Could not translate ftt drive id\n");    
    status = ftt_free_stat(ftt_stat_buff);
    return(-1);
  }

  if ( ds_translate_ftt_stats(ds_desc,ftt_stat_buff,INIT)  != 0 ) {
    printf("ds_init(): Error, Could not translate ftt stats\n");    
    status = ftt_free_stat(ftt_stat_buff);
    return(-1);
  }

  status = ftt_free_stat(ftt_stat_buff);

  if (ret_code == 0) {
    /* remember the ftt descriptor */
    ds_desc->ftt_d = ftt_d;
    _ds_set_ds_stats(&ds_desc->delta,0);
    _ds_set_ds_stats(&ds_desc->sum_of_deltas,0);
    _ds_set_ds_stats(&ds_desc->recent_stats,0);
  }

  return(ret_code);

}

int ds_update(DS_DESCRIPTOR* ds_desc, const ftt_descriptor ftt_d)
/************************************************************************
 *                                                                      *
 * ds_update                                                      *
 * routine to get the current drive statistics.                         *
 *                                                                      *
 ************************************************************************/
{
  ftt_stat_buf ftt_stat_buff;
  int status, ret_code;
  char* value;
  float float_value;

  DS_STATS* stats;

  ds_desc->recent_stats.init_flag = -1;

  if (  ds_desc->init_stats.init_flag != 1 ) {
    printf("ds_update(): Error, Not initialized, will not update\n");    
    return(-1);
  }

  if (  ds_desc->ftt_d != ftt_d ) {
    printf("ds_update(): Error, Different drive, will not update\n");    
    return(-1);
  }

  stats = &ds_desc->recent_stats;

  _ds_set_ds_stats(&ds_desc->recent_stats,-1);

  ret_code = 0;

  ftt_stat_buff = ftt_alloc_stat();
  status = ftt_get_stats(ds_desc->ftt_d,ftt_stat_buff);

  if (status != FTT_SUCCESS) {
    printf("ds_update(): Error, Could not get ftt stats, will not update\n");    
    status = ftt_free_stat(ftt_stat_buff);
    return(-1);
  }

  if ( ds_translate_ftt_stats(ds_desc,ftt_stat_buff,RECENT)  != 0 ) {
    printf("ds_update(): Error, Could not translate ftt stats, will not update\n");    
    status = ftt_free_stat(ftt_stat_buff);
    return(-1);
  }

  status = ftt_free_stat(ftt_stat_buff);

  if (ret_code == 0) {
    ds_desc->recent_stats.init_flag = 1;
  }

  return(ret_code);
}

void ds_print_stats(FILE* const fp,const char* const stat_name, const DS_STATS* const stats_pointer)
{
  /* aux function to print DS_STATS struct */

  fprintf(fp,"%s INIT FLAG: %d\n",stat_name,stats_pointer->init_flag); 
  fprintf(fp,"%s PWR HRS:   %d\n",stat_name,stats_pointer->power_hrs); 
  fprintf(fp,"%s MOT HRS:   %d\n",stat_name,stats_pointer->motion_hrs);
  fprintf(fp,"%s RD ERR:    %d\n",stat_name,stats_pointer->read_errors);
  fprintf(fp,"%s WR ERR:    %d\n",stat_name,stats_pointer->write_errors);
  fprintf(fp,"%s MB UREAD:  %d\n",stat_name,stats_pointer->mb_user_read);
  fprintf(fp,"%s MB UWRITE: %d\n",stat_name,stats_pointer->mb_user_write);
  fprintf(fp,"%s MB DREAD:  %d\n",stat_name,stats_pointer->mb_dev_read);
  fprintf(fp,"%s MB DWRITE: %d\n",stat_name,stats_pointer->mb_dev_write);
  fprintf(fp,"%s RETRIES:   %d\n",stat_name,stats_pointer->track_retries);
  fprintf(fp,"%s UNDERRUN:  %d\n",stat_name,stats_pointer->underrun);
  fprintf(fp,"%s MOUNT CT:  %d\n",stat_name,stats_pointer->mount_count);

}
/***************************************************************************
 *                                                                         *
 * ds_print()                                                        *
 *   Print stats in current drivestat descriptor to file.  If file is NULL,*
 *   reports are printed to stdout.  Return 0 on success, 1 on failure.    *
 *                                                                         *
 ***************************************************************************/

int ds_print(const DS_DESCRIPTOR* const ds_desc, const char* const file)
{
  FILE *fp;
  const int format_version = 21;

  if (file == NULL) {
    fp = stdout;
  }
  else {
    if ((fp = fopen(file,"w")) == NULL)
      return(-1);    
  }

  if ((fprintf(fp,"FORMAT VERSION:          %d\n",format_version)) <= 0) {
    if (fp != stdout) {
      fclose(fp);
    }
    return(-1);
  }
  
  
  fprintf(fp,"INIT FLAG:               %d\n",ds_desc->ds_init_flag);

  fprintf(fp,"DRIVE SERNO:             %s\n",ds_desc->drive_serial_number);
  fprintf(fp,"VENDOR:                  %s\n",ds_desc->vendor);
  fprintf(fp,"PROD TYPE:               %s\n",ds_desc->product_type);
  fprintf(fp,"LOGICAL NAME:            %s\n",ds_desc->logical_drive_name);

  fprintf(fp,"HOST:                    %s\n",ds_desc->host);
  fprintf(fp,"VOLSER:                  %s\n",ds_desc->tape_volser);

  fprintf(fp,"OPERATION:               %s\n",ds_desc->operation);
  fprintf(fp,"CLEANING BIT:            %d\n",ds_desc->cleaning_bit);

  ds_print_stats(fp,"INIT_STATS   ", &ds_desc->init_stats);
  ds_print_stats(fp,"DELTA_STATS  ", &ds_desc->delta);
  ds_print_stats(fp,"SUM_OF_DELTAS", &ds_desc->sum_of_deltas);
  ds_print_stats(fp,"RECENT_STATS ", &ds_desc->recent_stats);

  if (fp != stdout) {
    fclose(fp);
  }

  return(0);

}


int  _ds_readin_ds_stats(FILE* const fp, const char* const stat_name, DS_STATS* const stats_pointer)
{
  /* aux function to read in DS_STATS struct */
  char read_stat_name[50];

  if ( fscanf (fp,"%s INIT FLAG: %d\n",&read_stat_name,&stats_pointer->init_flag) <= 1 ) goto formaterror;
  if ( strcmp (read_stat_name,stat_name) != 0) goto formaterror;
  if ( fscanf (fp,"%s PWR HRS:   %d\n",&read_stat_name,&stats_pointer->power_hrs) <= 1 ) goto formaterror;
  if ( fscanf (fp,"%s MOT HRS:   %d\n",&read_stat_name,&stats_pointer->motion_hrs) <= 1 ) goto formaterror;
  if ( fscanf (fp,"%s RD ERR:    %d\n",&read_stat_name,&stats_pointer->read_errors) <= 1 ) goto formaterror;
  if ( fscanf (fp,"%s WR ERR:    %d\n",&read_stat_name,&stats_pointer->write_errors) <= 1 ) goto formaterror;
  if ( fscanf (fp,"%s MB UREAD:  %d\n",&read_stat_name,&stats_pointer->mb_user_read) <= 1 ) goto formaterror;
  if ( fscanf (fp,"%s MB UWRITE: %d\n",&read_stat_name,&stats_pointer->mb_user_write) <= 1 ) goto formaterror;
  if ( fscanf (fp,"%s MB DREAD:  %d\n",&read_stat_name,&stats_pointer->mb_dev_read) <= 1 ) goto formaterror;
  if ( fscanf (fp,"%s MB DWRITE: %d\n",&read_stat_name,&stats_pointer->mb_dev_write) <= 1 ) goto formaterror;
  if ( fscanf (fp,"%s RETRIES:   %d\n",&read_stat_name,&stats_pointer->track_retries) <= 1 ) goto formaterror;
  if ( fscanf (fp,"%s UNDERRUN:  %d\n",&read_stat_name,&stats_pointer->underrun) <= 1 ) goto formaterror;
  if ( fscanf (fp,"%s MOUNT CT:  %d\n",&read_stat_name,&stats_pointer->mount_count) <= 1 ) goto formaterror;
  if ( strcmp (read_stat_name,stat_name) != 0) goto formaterror;

  return (0);

 formaterror:
  printf("_ds_readin_ds_stats(): Error, Could not read in %s\n", stat_name );
  return(-1);  

}

int ds_readin (DS_DESCRIPTOR* const ds_desc, const char* const file)
{
  FILE *fp;
  int ret_code;
  const int format_version = 21;
  int read_format_version;
  char buf[40];

  if ((fp = fopen(file,"r")) == NULL)    return(-1);  

  if ( fscanf (fp,"FORMAT VERSION:          %d\n",&read_format_version) <= 0 ) goto formaterror;
  if ( read_format_version != format_version ) goto formaterror;

  if ( fscanf (fp,"INIT FLAG:               %d\n",&ds_desc->ds_init_flag) <= 0 ) goto formaterror;

  if ( fscanf (fp,"DRIVE SERNO:             %s\n",&ds_desc->drive_serial_number) <= 0 ) goto formaterror;
  if ( fscanf (fp,"VENDOR:                  %s\n",&ds_desc->vendor) <= 0 ) goto formaterror;
  if ( fscanf (fp,"PROD TYPE:               %s\n",&ds_desc->product_type) <= 0 ) goto formaterror;
  if ( fscanf (fp,"LOGICAL NAME:            %s\n",&ds_desc->logical_drive_name) <= 0 ) goto formaterror;

  if ( fscanf (fp,"HOST:                    %s\n",&ds_desc->host) <= 0 ) goto formaterror;
  if ( fscanf (fp,"VOLSER:                  %s\n",&ds_desc->tape_volser) <= 0 ) goto formaterror;

  if ( fscanf (fp,"OPERATION:               %s\n",&ds_desc->operation) <= 0 ) goto formaterror;
  if ( fscanf (fp,"CLEANING BIT:            %d\n",&ds_desc->cleaning_bit) <= 0 ) goto formaterror;

  /* note no spaces in the strings below */

  if ( _ds_readin_ds_stats(fp,"INIT_STATS", &ds_desc->init_stats) != 0 ) goto formaterror;
  if ( _ds_readin_ds_stats(fp,"DELTA_STATS", &ds_desc->delta) != 0 ) goto formaterror;
  if ( _ds_readin_ds_stats(fp,"SUM_OF_DELTAS", &ds_desc->sum_of_deltas) != 0 ) goto formaterror;
  if ( _ds_readin_ds_stats(fp,"RECENT_STATS", &ds_desc->recent_stats) != 0 ) goto formaterror;  

  fclose(fp);

  return (0);

 formaterror:
  fclose(fp);
  return(-1);  

}

int _ds_set_flag_pointer(DS_DESCRIPTOR* const ds_desc, DS_STATS** p_stats_pointer, int** p_flag_pointer, const int flag){

  /* aux function to set stats_pointer & flag_pointer */

  if (flag == DELTA) {
    *p_stats_pointer = &ds_desc->delta;
    *p_flag_pointer  = &ds_desc->delta.init_flag;
  } else {
    if (flag == SUM_OF_DELTAS) {
      *p_stats_pointer = &ds_desc->sum_of_deltas;
      *p_flag_pointer  = &ds_desc->sum_of_deltas.init_flag;
    } else {
      if (flag == INIT) {
	*p_stats_pointer = &ds_desc->init_stats;
	*p_flag_pointer  = &ds_desc->init_stats.init_flag;
      } else {
	if (flag == RECENT) {
	  *p_stats_pointer = &ds_desc->recent_stats;
	  *p_flag_pointer  = &ds_desc->recent_stats.init_flag;
	} else {
	  return (-1);
	}
      }
    }
  }

  return (0);
}


DS_STATS *ds_alloc_stats()
{
  return (DS_STATS*) malloc(sizeof(DS_STATS));
}

void ds_free_stats(DS_STATS* dss)
{
  free(dss);
}


int ds_set_stats(DS_DESCRIPTOR* const ds_desc, const DS_STATS* const stat_buf, const int flag)
{

  /* function to set DS_STATS struct of DS_DESCRIPTOR 
   */

  DS_STATS *stats_pointer;
  int *flag_pointer;

  if ( _ds_set_flag_pointer(ds_desc, &stats_pointer, &flag_pointer, flag) != 0 ) {
    printf("ds_set_stats(): Error, Wrong flag, stats not set\n");
    return (-1);
  }

  stats_pointer->power_hrs     = stat_buf->power_hrs;
  stats_pointer->motion_hrs    = stat_buf->motion_hrs;
  stats_pointer->read_errors   = stat_buf->read_errors;
  stats_pointer->mb_user_read  = stat_buf->mb_user_read;
  stats_pointer->mb_user_write = stat_buf->mb_user_write;
  stats_pointer->mb_dev_read   = stat_buf->mb_dev_read;
  stats_pointer->mb_dev_write  = stat_buf->mb_dev_write;
  stats_pointer->write_errors  = stat_buf->write_errors;
  stats_pointer->track_retries = stat_buf->track_retries;
  stats_pointer->underrun      = stat_buf->underrun;
  stats_pointer->mount_count   = stat_buf->mount_count;

  *flag_pointer = 1;

  return (0);

}

int ds_extract_stats(DS_DESCRIPTOR* const ds_desc, DS_STATS* const stat_buf, const int flag)
{

  /* function to get DS_STATS struct of DS_DESCRIPTOR */

  DS_STATS* stats_pointer;
  int* flag_pointer;

  if ( _ds_set_flag_pointer(ds_desc, &stats_pointer, &flag_pointer, flag) != 0 ) {
    printf("ds_extract_stats(): Error, Wrong flag\n");
    return (-1);
  }

  if (*flag_pointer != 1) {
    printf("ds_extract_stats(): Error, Stats not set\n");
    return (-1);
  }

  stat_buf->init_flag     = stats_pointer->init_flag;
  stat_buf->power_hrs     = stats_pointer->power_hrs;
  stat_buf->motion_hrs    = stats_pointer->motion_hrs;
  stat_buf->read_errors   = stats_pointer->read_errors;
  stat_buf->mb_user_read  = stats_pointer->mb_user_read;
  stat_buf->mb_user_write = stats_pointer->mb_user_write;
  stat_buf->mb_dev_read   = stats_pointer->mb_dev_read;
  stat_buf->mb_dev_write  = stats_pointer->mb_dev_write;
  stat_buf->write_errors  = stats_pointer->write_errors;
  stat_buf->track_retries = stats_pointer->track_retries;
  stat_buf->underrun      = stats_pointer->underrun;
  stat_buf->mount_count   = stats_pointer->mount_count;

  return (0);

}


int ds_bump_deltasum(DS_DESCRIPTOR* const ds_desc, const DS_STATS* const stat_buf)
/*************************************************************************
 *                                                                       *
 * ds_bump_deltasum()                                                     *
 *    bump the delta sum portion of the ds_descriptor with the data passed*
 *    in the stat_buf.                                                   *
 *                                                                       *
 *************************************************************************/
{

  if ( ds_desc->sum_of_deltas.init_flag != 1) {
    printf("ds_bump_deltasum(): Error, sum of deltas not set, will not bump\n");
    return (-1);
  }

  ds_desc->sum_of_deltas.power_hrs += stat_buf->power_hrs;
  ds_desc->sum_of_deltas.motion_hrs += stat_buf->motion_hrs;
  ds_desc->sum_of_deltas.read_errors += stat_buf->read_errors;
  ds_desc->sum_of_deltas.mb_user_read += stat_buf->mb_user_read;
  ds_desc->sum_of_deltas.mb_user_write += stat_buf->mb_user_write;
  ds_desc->sum_of_deltas.mb_dev_read += stat_buf->mb_dev_read;
  ds_desc->sum_of_deltas.mb_dev_write += stat_buf->mb_dev_write;
  ds_desc->sum_of_deltas.write_errors += stat_buf->write_errors;
  ds_desc->sum_of_deltas.track_retries += stat_buf->track_retries;
  ds_desc->sum_of_deltas.underrun += stat_buf->underrun;
  ds_desc->sum_of_deltas.mount_count += stat_buf->mount_count;

  return (0);

}

int ds_set_character_field(DS_DESCRIPTOR* ds_desc,const char* const string, const int field)
{
  if (field == DRIVE_SERIAL_NUMBER) {
    strcpy(ds_desc->drive_serial_number,string);
  } else {
    if (field == VENDOR) {
      strcpy(ds_desc->vendor,string);
    } else {
      if (field == PRODUCT_TYPE)  {
	strcpy(ds_desc->product_type,string);
      } else {
	if (field == OPERATION)  {
	  strcpy(ds_desc->operation,string);
	} else {
	  if (field == TAPE_VOLSER)  {
	    strcpy(ds_desc->tape_volser,string);
	  } else {
	    if (field == HOST)  {
	      strcpy(ds_desc->host,string);
	    } else {
	      if (field == LOGICAL_DRIVE_NAME)  {
		strcpy(ds_desc->logical_drive_name,string);
	      } else {
		return (-1);
	      }
	    }
	  }
	}
      }
    }
  }
  
  return (0);
}


int ds_compute_delta(DS_DESCRIPTOR* ds_desc)

/************************************************************************
 *                                                                      *
 * ds_compute_delta                                                     *
 *   computes the delta based                                           *
 *   on the current stats and the stats in the recent portion of the    *
 *   descriptor. Will replace the  init_stats with recent_stats &       *
 *   calculate the sum of deltas as well                                *
 *                                                                      *
 ************************************************************************/
{

  if ( ds_desc->recent_stats.init_flag != 1 || ds_desc->init_stats.init_flag !=1 ) {
    printf("ds_compute_delta(): Error, Not initialized/updated, will not compute\n");    
    return(-1);
  }
  
  /* beware power & motion hours increase slowly deltas are 0 most of the time...*/

  /* compute delata */

  ds_desc->delta.power_hrs     = ds_desc->recent_stats.power_hrs     - ds_desc->init_stats.power_hrs;
  ds_desc->delta.motion_hrs    = ds_desc->recent_stats.motion_hrs    - ds_desc->init_stats.motion_hrs;
  ds_desc->delta.mb_user_read  = ds_desc->recent_stats.mb_user_read  - ds_desc->init_stats.mb_user_read;
  ds_desc->delta.mb_user_write = ds_desc->recent_stats.mb_user_write - ds_desc->init_stats.mb_user_write;
  ds_desc->delta.mb_dev_read   = ds_desc->recent_stats.mb_dev_read   - ds_desc->init_stats.mb_dev_read;
  ds_desc->delta.mb_dev_write  = ds_desc->recent_stats.mb_dev_write  - ds_desc->init_stats.mb_dev_write;
  ds_desc->delta.read_errors   = ds_desc->recent_stats.read_errors   - ds_desc->init_stats.read_errors;
  ds_desc->delta.write_errors  = ds_desc->recent_stats.write_errors  - ds_desc->init_stats.write_errors;
  ds_desc->delta.track_retries = ds_desc->recent_stats.track_retries - ds_desc->init_stats.track_retries;
  ds_desc->delta.underrun      = ds_desc->recent_stats.underrun      - ds_desc->init_stats.underrun;
  ds_desc->delta.mount_count   = ds_desc->recent_stats.mount_count   - ds_desc->init_stats.mount_count;
  
  ds_desc->delta.init_flag = 1;

  /* compute the sum of deltas */

  ds_desc->sum_of_deltas.power_hrs     += ds_desc->delta.power_hrs;
  ds_desc->sum_of_deltas.motion_hrs    += ds_desc->delta.motion_hrs;
  ds_desc->sum_of_deltas.read_errors   += ds_desc->delta.read_errors;
  ds_desc->sum_of_deltas.mb_user_read  += ds_desc->delta.mb_user_read;
  ds_desc->sum_of_deltas.mb_user_write += ds_desc->delta.mb_user_write;
  ds_desc->sum_of_deltas.mb_dev_read   += ds_desc->delta.mb_dev_read;
  ds_desc->sum_of_deltas.mb_dev_write  += ds_desc->delta.mb_dev_write;
  ds_desc->sum_of_deltas.write_errors  += ds_desc->delta.write_errors;
  ds_desc->sum_of_deltas.track_retries += ds_desc->delta.track_retries;
  ds_desc->sum_of_deltas.underrun      += ds_desc->delta.underrun;
  ds_desc->sum_of_deltas.mount_count   += ds_desc->delta.mount_count;

  ds_desc->sum_of_deltas.init_flag = 1;

  /* assing recent_stats to init_stats */

  ds_desc->init_stats.power_hrs     = ds_desc->recent_stats.power_hrs;
  ds_desc->init_stats.motion_hrs    = ds_desc->recent_stats.motion_hrs;
  ds_desc->init_stats.mb_user_read  = ds_desc->recent_stats.mb_user_read;
  ds_desc->init_stats.mb_user_write = ds_desc->recent_stats.mb_user_write;
  ds_desc->init_stats.mb_dev_read   = ds_desc->recent_stats.mb_dev_read;
  ds_desc->init_stats.mb_dev_write  = ds_desc->recent_stats.mb_dev_write;
  ds_desc->init_stats.read_errors   = ds_desc->recent_stats.read_errors;
  ds_desc->init_stats.write_errors  = ds_desc->recent_stats.write_errors;
  ds_desc->init_stats.track_retries = ds_desc->recent_stats.track_retries;
  ds_desc->init_stats.underrun      = ds_desc->recent_stats.underrun;
  ds_desc->init_stats.mount_count   = ds_desc->recent_stats.mount_count;

  return (0);

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
int ds_prepare_list(const char* const drive,const char* const vendor,
		    const char* const prod_type,
		    const char* const host, const char* const vsn,
		    const char* const bdate,
		    const char* const edate,int *n)
{
  char msg[4096];
  char send_buff[4096];
  char ack_msg[512];
  char s_time_stamp[512];
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

  sprintf (s_time_stamp,"%d",time(NULL));
  strcat(msg, s_time_stamp);
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
  printf("sending request...\n");
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
  buff = (char* ) malloc(n + 1);

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

 
void _ds_set_string_if_ref_is_empty(const char* const ref, char* string, const char* const value)
{
  /* aux function */
  if (strlen(ref) == 0) {
    strcpy(string,value);
  } else {
    strcpy(string,ref);
  }
}
int ds_send_stats(const DS_DESCRIPTOR* const ds_desc, const int timeout, const int flag)
/************************************************************************
 *                                                                      *
 * ds_send_stats()                                                      *
 *   Send the stats to the drivestat server.                            *
 *   <timeout> if non-0 will cause the command to block for             *
 *   <timeout> seconds. (not yet...)                                    *
 *   <flag> instructs  what to send:                                    *
 *      DELTA - Send the delta in the ds_descriptor .                   *
 *      SUM_OF_DELTAS - Send the sum of deltas in the  descriptor.
 *      ABSOLUTE                                                  send  *
 *      BUMP_MOUNTS                                                                *
 *   The format of the packet sent will be:                             *
 *     01|<DSN>|<VEN>|<PRD>|<TIMESTAMP>|<TAPEVS>|<PHRS>|<MHRS>|<CBIT>|<USRREAD>|    *
 *     <USRWR>|<DEVREAD>|<DEVWRITE>|<RDERR>|<WRERR>|<RET>|<UND>|<MTCT>  *
 *     <HOST>|<LOGICAL_DRIVE>^                                          *
 *   Return 0 on success, -1 on error.                                  *
 *                                                                      *
 *   The ds_server port and host should be stored in the users          *
 *   environment (DS_SERVER_PORT, DS_SERVER_HOST).  This is established *
 *   if the user does a ups setup.                                      *
 *                                                                      *
 ************************************************************************/ 
{
  char send_buff[4096],
    msg[4096],
    s[64];

  char stat_type[MAX_STAT_TYPE_SIZE + 1];
  char host[MAX_HOST_LEN + 1];
  char logical_drive_name[MAX_LOGICAL_DRIVE_NAME_LEN + 1];
  char tape_volser[MAX_VOLSER_LEN + 1];
  int mount_count;

  int sd;
  int rc;

  const DS_STATS* stats_pointer;

  time_stamp=time(NULL);

  if ( (flag & DELTA) && (flag & ABSOLUTE) ||
       (flag & SUM_OF_DELTAS) && (flag & ABSOLUTE) ||
       (flag & DELTA) && (flag & SUM_OF_DELTAS) ) {
    printf("ds_send_stats(): Error: Conflicting flags\n");
    return (-1);
  }

  if ( (flag & DELTA) || (flag & ABSOLUTE) || (flag & SUM_OF_DELTAS) ) {

    /* power & motion hours are always absolute as deltas are 0 most of the time...*/

    if (flag & DELTA) {
      if (ds_desc->delta.init_flag == 1) {
	stats_pointer = &ds_desc->delta;
	strcpy(stat_type,"DELTA");
      }
      else {
	printf("ds_send_stats(): Error, No stats gathered?\n");
	return(-1);
      }
    }
    else if (flag & SUM_OF_DELTAS) {
      if (ds_desc->sum_of_deltas.init_flag == 1) {
	stats_pointer = &ds_desc->sum_of_deltas;
	strcpy(stat_type,"SUM_OF_DELTAS");
      }
      else {
	printf("ds_send_stats(): Error, No stats gathered?\n");
	return (-1);
      }
    }
    else if (flag & ABSOLUTE) {
      strcpy(stat_type,"ABSOLUTE");
      if (ds_desc->recent_stats.init_flag == 1) {
	stats_pointer = &ds_desc->recent_stats;
      }
      else if (ds_desc->init_stats.init_flag == 1) {
	stats_pointer = &ds_desc->init_stats;
      }
      else {
	printf("ds_send_stats(): Error, No stats gathered?\n");
	return(-1);
      }
    }
    else {
      printf("ds_send_stats(): Error, unknown flag?\n");
      return(-1);
    }

    _ds_set_string_if_ref_is_empty(ds_desc->host,host,"UNKNOWN");
    _ds_set_string_if_ref_is_empty(ds_desc->logical_drive_name,logical_drive_name,"UNKNOWN");
    _ds_set_string_if_ref_is_empty(ds_desc->tape_volser,tape_volser,"UNKNOWN");
    
/*     if (strcmp(ds_desc->operation,"UNKNOWN") != 0) */
/*       strcpy(stat_type,ds_desc->operation); */

    sprintf(msg,
	    "%s|%s|%s|%d|%s|%s|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%d|%s|%s",
	    ds_desc->drive_serial_number,
	    ds_desc->vendor,
	    ds_desc->product_type,
	    time_stamp,
	    tape_volser,
	    stat_type,
	    stats_pointer->power_hrs,
	    stats_pointer->motion_hrs,
	    ds_desc->cleaning_bit,
	    stats_pointer->mb_user_read,
	    stats_pointer->mb_user_write,
	    stats_pointer->mb_dev_read,
	    stats_pointer->mb_dev_write,
	    stats_pointer->read_errors,
	    stats_pointer->write_errors,
	    stats_pointer->track_retries,
	    stats_pointer->underrun,
	    stats_pointer->mount_count,
	    host,
	    logical_drive_name);

    if (flag & ABSOLUTE) {
      sprintf(send_buff,"%d|%s|%s", 
	      strlen(msg),AUPDATE_STR,msg);
    }
    else {
      sprintf(send_buff,"%d|%s|%s", 
	      strlen(msg),UPDATE_STR,msg);
    }

    sd = connect_to_server();
    /* printf("ds_send_stats(): send_buff = %s\n",send_buff); */
    if (sd > 0) {
      rc = send_data(sd,send_buff);
      if (rc > 0) {
	rc = get_ack(sd);
      }
      if (rc < 0) {
	printf("ds_send_stats(): Error, Server reported error\n");
	return (rc);
      }
    }
    else {
      printf("ds_send_stats(): Error, could not connect to the server. code=%d\n",sd);
      return(sd);
    }
    close(sd);
  }
  
  if (flag & BUMP_MOUNTS) {
    mount_count = 1;
    sprintf(msg,
	    "%s|%s|%s|%d|%d",
	    ds_desc->drive_serial_number,
	    ds_desc->vendor,
	    ds_desc->product_type,
	    time_stamp,
	    mount_count);
    sprintf(send_buff,"%d|%s|%s",
	    strlen(msg),BUMP_MOUNTS_STR,msg);
    sd = connect_to_server();
    /* printf("ds_send_stats(): send_buff = %s\n",send_buff);  */
    if (sd > 0) {
      rc = send_data(sd,send_buff);
      if (rc > 0) {
	rc = get_ack(sd);
      }
      if (rc < 0) {
        printf("ds_send_stats():Error, Server reported error\n");
        return (rc);
      }
    }
    else {
      printf("ds_send_stats(): Error, send_data bump mount failed. code=%d\n",sd);
      return(sd);
    }
    close(sd);
  }

  return(0);

}

/************************************************************************
 *                                                                      *
 * ds_drive_maintenace()                                                *
 *   Reflect maintenace on the drive in the db                          *
 *     INSTALL                                                          *
 *     REPAIR                                                           *
 *     CLEAN                                                            *
 *                                                                      *
 ************************************************************************/

int ds_drive_maintenance(const DS_DESCRIPTOR* const ds_desc,const int flag,
			 const char* const host,const char* const logical_drive_name)
{

  char send_buff[4096],
      msg[4096];
  char hostn[MAX_HOST_LEN + 1];
  char ldn[MAX_LOGICAL_DRIVE_NAME_LEN + 1];
  const char* hostp;
  const char* ldnp;
  int sd;

  if ( ds_desc->ds_init_flag != 1 ) {
    printf("ds_drive_maintenance(): Error, Set drive id first, stats not send\n");
    return (-1);
  }  

  time_stamp=time(NULL);

  if (host != NULL) {
      hostp = host;
  } else if (ds_desc->host != NULL) {
      hostp = ds_desc->host;
  } else {
      strcpy(hostn,"UNKNOWN");
      hostp = hostn;
  }

  if (logical_drive_name != NULL) {
      ldnp = logical_drive_name;
  } else if (ds_desc->logical_drive_name != NULL) {
      ldnp = ds_desc->logical_drive_name;
  } else {
      strcpy(ldn,"UNKNOWN");
      ldnp = ldn;
  }

  if (flag == INSTALL) {
    sprintf(msg,
	    "%s|%s|%s|%d|%s|%s",
	    ds_desc->drive_serial_number,
	    ds_desc->vendor,
	    ds_desc->product_type,
	    time_stamp,
	    hostp,
	    ldnp);
    sprintf(send_buff,"%d|%s|%s^",
	    strlen(msg),INSTALL_STR,msg);
  }
  else if ((flag == REPAIR || flag == CLEAN)) {
    sprintf(msg,"%s|%s|%s|%d",
	    ds_desc->drive_serial_number,
	    ds_desc->vendor,
	    ds_desc->product_type,
	    time_stamp);
    if (flag == REPAIR)
      sprintf(send_buff,"%d|%s|%s^",
	      strlen(msg),REPAIR_STR,msg);
    else 
      sprintf(send_buff,"%d|%s|%s^",
	      strlen(msg),CLEAN_STR,msg);
  }
  else {
    printf("ds_drive_maintenance(): Error, unknown maintanance operation\n");
    return(-1);
  }
  
  printf("ds_drive_maintenance(): send_buff = %s\n",send_buff);
  sd = connect_to_server();
  send_data(sd,send_buff);
  if (sd > 0) {
    close(sd);
    return(0);
  }
  else {
    printf("ds_drive_maintenance(): Error, send_data failed. code=%d\n",sd);
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
  char* serv_host,*serv_port;
  int serv_tcp_port;
  int sd;

  serv_host = getenv("DS_SERVER_HOST");

  if (!serv_host) {
    printf("Error, No server_host\n");
    return(-1);
  }

  serv_port = getenv("DS_SERVER_PORT");
  if (!serv_port) {
    printf("Error, No server port\n");
    return(-1);
  }

  serv_tcp_port = atoi(serv_port);
  bzero((char* ) &serv_addr, sizeof(serv_addr));

  if (phe=(struct hostent *)gethostbyname(serv_host))
    bcopy(phe->h_addr,&serv_addr.sin_addr,phe->h_length);
  else {
    printf("Error, Name %s not in name server\n",serv_host);
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

int send_data(const int sd,const char* const buf)
{
  int nleft, nwritten;
  char* ptr,
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

int _write_sock(const int sd, const char* const buf)
{
  int nwritten,nleft,n=0;
  const char* ptr;

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


int get_ack(const int sd)
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
    printf("Error, ERROR read status =%d\n",rc);
    return -2;
  }

  if (rc == 0) {
    printf("Error, Premature EOF encountered - did not get ACK in get_ack\n");
    sleep(20);
    return -1;
  }

  i = 0;
  while (ack_msg[i] != END_OF_MESSAGE) {
    i++;
    rc = read(sd,&ack_msg[i],1);
    nread+=rc;
    if (rc <= 0) {
	printf("Error, ERROR read in loop status,i = %d/%d\n",rc,i);
      return -3;
    }
  }
  ack_msg[i] = '\0';
  ack_return_code = atoi(&ack_msg[4]);
  return(ack_return_code); 
}
