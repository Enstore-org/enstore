/*************************************************************
 *
 * $Id$
 *
 *************************************************************/

%module drivestat

%{
#include <string.h>
#include "drivestat.h"
#include "ds_api.h"
#include "ftt.h"

int ds_print_report(void) {
   DS_REPORT *report;
   int n;
   int sd;
   int i;
  
   sd = ds_prepare_list(NULL,NULL,NULL,NULL,NULL,NULL,NULL,&n); 
   if ( sd < 0) {
       printf("ERROR ds_prepare_list status = %d\n",sd);
       return(-1);
   }

   for (i=0;i<n;i++) {
    report = ds_getnext(sd);
    if (report != NULL) {
       printf("Drive Serial Number: %s\n",report->drive_serial_number);
       printf("Vendor: %s\n",report->vendor);
       printf("Product Type: %s\n",report->product_type);
       printf("Tape Volser: %s\n",report->tape_volser);
       printf("Operation: %s\n",report->operation);
       printf("Power Hrs: %d\n",report->power_hrs);
       printf("Motion Hrs: %d\n",report->motion_hrs);
       printf("Cleaning Bit: %d\n",report->cleaning_bit);
       printf("mb_user_read: %d\n",report->mb_user_read);
       printf("mb_user_write: %d\n",report->mb_user_write);
       printf("mb_dev_read: %d\n",report->mb_dev_read);
       printf("mb_dev_write: %d\n",report->mb_dev_write);
       printf("read errs: %d\n",report->read_errs);
       printf("write errs: %d\n",report->write_errs);
       printf("track retries: %d\n",report->track_retries);
       printf("underrun: %d\n",report->underrun);
    } else {
       break;
    }
    ds_close_list(sd);
   }
}


%}

#define CLEAN 3 
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


/*************************************************************************
 *                                                                       *
 * ds_alloc()                                                            *
 *   allocate drivestat descriptor                                       *
 *                                                                       *
 *************************************************************************/
DS_DESCRIPTOR *ds_alloc();

/*************************************************************************
 *                                                                       *
 * ds_translate_ftt_drive_id()                                           *
 *   translate ftt drive id                                              *
 *                                                                       *
 *************************************************************************/
int ds_translate_ftt_drive_id(DS_DESCRIPTOR*,ftt_stat_buf);

/*************************************************************************
 *                                                                       *
 * ds_translate_ftt_stats()                                              *
 *   translate ftt stats                                                 *
 *                                                                       *
 *************************************************************************/
int ds_translate_ftt_stats(DS_DESCRIPTOR* ds_desc,ftt_stat_buf ftt_stat_buff,int flag);

/*************************************************************************
 *                                                                       *
 * ds_free()                                                             *
 *   deallocate drivestat descriptor                                     *
 *                                                                       *
 *************************************************************************/
void ds_free(DS_DESCRIPTOR* ds_desc);

/*************************************************************************
 *                                                                       *
 * ds_init()                                                             *
 *   initialize drivestat descriptor                                     *
 *                                                                       *
 *************************************************************************/
int ds_init(DS_DESCRIPTOR* ds_desc, ftt_descriptor ftt_d);

/*************************************************************************
 *                                                                       *
 * ds_update()                                                           *
 *   update drivestat descriptor                                         *
 *                                                                       *
 *************************************************************************/
int ds_update(DS_DESCRIPTOR* ds_desc, ftt_descriptor ftt_d);


/*************************************************************************
 *                                                                       *
 * ds_get_stats_buff()                                                   *
 *   Create a drivestat DS_STATS buffer                                  *
 *                                                                       *
 *************************************************************************/
/*DS_STATS * ds_get_stats_buff(void);*/

/*************************************************************************
 *                                                                       *
 * ds_print_report()                                                     *
 *   Print every record in the database                                  *
 *                                                                       *
 *************************************************************************/
int ds_print_report(void);

/****************************************************************************
 *                                                                          *
 * ds_init_stats()                                                          *
 *   Make a call to ftt and get the initial statistics to be used to compute*
 *   the deltas.                                                            *
 *                                                                          *
 ****************************************************************************/
#int ds_init_stats(DS_DESCRIPTOR *d);

/***************************************************************************
 *                                                                         *
 * ds_print()                                                              *
 *   Print ds_decriptorto file.  If file is NULL,                          *
 *   reports are printed to stdout.  Return 0 on success, 1 on failure.    *
 *                                                                         *
 ***************************************************************************/
%typemap(python, in) char *file {
    if ($source == Py_None)
	 $target = (char *)0;
    else
	 $target = PyString_AsString($source);
}
int ds_print(const DS_DESCRIPTOR* ds_desc, char* file);

/***************************************************************************
 *                                                                         *
 * ds_print_stats()                                                        *
 *   Print stats in current drivestat descriptor to file.  If file is NULL,*
 *   reports are printed to stdout.  Return 0 on success, 1 on failure.    *
 *                                                                         *
 ***************************************************************************/
%typemap(python, in) FILE * {
        if (!PyFile_Check($source)) {
            PyErr_SetString(PyExc_TypeError, "Expected file object");
            return NULL;
        }
        $target = PyFile_AsFile($source);
}
void ds_print_stats(FILE* fp,const char* stat_name, DS_STATS* stats_pointer);


/***************************************************************************
 *                                                                         *
 * ds_set_character_field()                                                *
 *   set a character field in ds descriptor                                *
 *                                                                         *
 ***************************************************************************/
int ds_set_character_field(DS_DESCRIPTOR* ds_desc,char* string, int field);

/***************************************************************************
 *                                                                         *
 * ds_set_stats()                                                          *
 *   set stats                                                             *
 *                                                                         *
 ***************************************************************************/
int ds_set_stats(DS_DESCRIPTOR* ds_desc, DS_STATS* stat_buf, int flag);

/***************************************************************************
 *                                                                         *
 * ds_extract_stats()                                                      *
 *   extract stats                                                         *
 *                                                                         *
 ***************************************************************************/
int ds_extract_stats(DS_DESCRIPTOR* ds_desc, DS_STATS* stat_buf, int flag);

/***************************************************************************
 *                                                                         *
 * ds_bump_deltasum()                                                      *
 *   bump delta sum                                                        *
 *                                                                         *
 ***************************************************************************/
int ds_bump_deltasum(DS_DESCRIPTOR* ds_desc, DS_STATS* stat_buf);

/************************************************************************
 *                                                                      *
 * ds_compute_delta                                                     *
 *   Gathers the stats from the tape drive, and computes the delta based*
 *   on the current stats and the stats in the init portion of the      *
 *   descriptor.                                                        *
 *                                                                      *
 ************************************************************************/
int ds_compute_delta(DS_DESCRIPTOR *d);

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
		    char *edate,int *n);

/************************************************************************
 *                                                                      *
 * ds_getnext()                                                         *
 *  Fetches the next record from the list prepared by ds_prepare_list().*
 *                                                                      *
 ************************************************************************/
DS_REPORT *ds_getnext(int list_sd);

/************************************************************************
 *                                                                      *
 * ds_close_list()                                                      *
 *    Shuts down the connection to the list processing process of the   *
 *    server.                                                           *
 *                                                                      *
 ************************************************************************/
int ds_close_list(int list_sd);

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
int ds_send_stats(DS_DESCRIPTOR *d, int timeout, int flag);

/************************************************************************
 *                                                                      *
 * ds_drive_maintenace()                                                *
 *   Perform maintenace on the drive:                                   *
 *     INSTALL                                                          *
 *     REPAIR                                                           *
 *     CLEAN                                                            *
 *                                                                      *
 ************************************************************************/

%typemap(python, in) char *host1 {
    if ($source == Py_None)
	 $target = (char *)0;
    else
	 $target = PyString_AsString($source);
}

%typemap(python, in) char *logical_drive_name1 {
    if ($source == Py_None)
	 $target = (char *)0;
    else
	 $target = PyString_AsString($source);
}

int ds_drive_maintenance(DS_DESCRIPTOR *d,int flag,
			 char *host1,char *logical_drive_name1);

/************************************************************************
 *                                                                      *
 * connect_to_server()                                                  *
 *   connect to drivestat server.                                       *
 *                                                                      *
 ************************************************************************/
int connect_to_server(char *serv_host,char *serv_port);

/************************************************************************
 *                                                                      *
 * send_data()                                                          *
 *   Send the data to the server.  On success, it returns the socket    *
 *   descriptor.                                                        *
 *                                                                      *
 ************************************************************************/
int send_data(char *buf);

/*****************************************************************************
 *                                                                           *
 * _write_sock(sd,buff)                                                      *
 *    Utility routine to write n bytes over a socket.                        *
 *                                                                           *
 *****************************************************************************/
int _write_sock(int sd,char *buf);

