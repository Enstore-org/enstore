static char rcsid[] = "@(#) $Id$";


#ifndef WIN32
#include <unistd.h>
#endif

#include <stdlib.h>
#include <sys/utsname.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>
#include <ftt_private.h>
#include <sys/mtio.h>

int ftt_open_io_dev(ftt_descriptor d);
int ftt_scsi_set_compression(ftt_descriptor d, int compression);

int
ftt_status(ftt_descriptor d, int time_out) {
    return ftt_stats_status(d,time_out);
}

int
ftt_set_hwdens(ftt_descriptor d, int hwdens) {
    /* ignore hwdens, 'cause we opened the right device node */
   return 0;
}

int 
ftt_set_compression(ftt_descriptor d, int compression) {

  struct mtop mt;
  int         rc;


   static int recursing = 0;
   int res=0;

   if ( !recursing ) {
       recursing = 1;
       res = ftt_open_dev(d);
       recursing = 0;
       if (res > 0) {

            mt.mt_op = MTCOMPRESSION;
            mt.mt_count = compression;
            res = ioctl(d->file_descriptor, MTIOCTOP, &mt);
       }
   }

  return res;
  /* return ftt_scsi_set_compression(d, compression); */
}

int
ftt_set_blocksize(ftt_descriptor d, int blocksize) {

  return 0;
}

int
ftt_get_hwdens(ftt_descriptor d, char *devname) {
    int res;

    res = d->devinfo[d->which_is_default].hwdens;
    return res;
}
