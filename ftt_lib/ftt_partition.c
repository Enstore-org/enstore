static char rcsid[] = "@(#)$Id$";
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "ftt_private.h"

ftt_partbuf 	
ftt_alloc_parts() {
     ftt_partbuf res;
     res = malloc(sizeof(*res));
     if (res != 0) {
            memset(res, sizeof(*res),0);
     }
     return res;
}

void 		
ftt_free_parts(ftt_partbuf p) {
   free(p);
}

int 		
ftt_extract_nparts(ftt_partbuf p) {
   return p->n_parts;
}

int 		
ftt_extract_maxparts(ftt_partbuf p) {
   return p->max_parts;
}

long 		
ftt_extract_part_size(ftt_partbuf p,int n) {
    if ( n > p->n_parts || n < 0) {
        ftt_eprintf("not that many partitions in buffer");
	ftt_errno = FTT_EFAULT;
        return -1;
    }
    return p->partsizes[n];
}

int 		
ftt_set_nparts(ftt_partbuf p,int n) {
    if ( n < p->max_parts) {
	p->n_parts = n;
        return 0;
    } else {
        return -1;
    }
}

int 		
ftt_set_part_size(ftt_partbuf p,int n,long sz) {
    if ( n > p->n_parts || n < 0) {
        ftt_eprintf("not that many partitions in buffer");
	ftt_errno = FTT_EFAULT;
        return -1;
    }
    p->partsizes[n] = sz;
    return 0;
}
#define pack(a,b,c,d) \
     (((unsigned long)(a)<<24) + ((unsigned long)(b)<<16) + ((unsigned long)(c)<<8) + (unsigned long)(d))


int		
ftt_get_partitions(ftt_descriptor d,ftt_partbuf p) {
    static char buf[136];
    static unsigned char cdb_modsen11[6] = {0x1a, 0x08, 0x11, 0x00,136, 0x00};
    int res;
    int i;

    res = ftt_do_scsi_command(d,"Get Partition table", cdb_modsen11, 6, buf, 136, 10, 0);
    if (res < 0) return res;
    p->n_parts = buf[4+3];
    p->max_parts = buf[4+2];
    for( i = 0 ; i <= p->n_parts; i++ ) {
        p->partsizes[i] = pack(0,0,buf[4+8+2*i],buf[4+8+2*i+1]);
    }
    return 0;
}

int		
ftt_write_partitions(ftt_descriptor d,ftt_partbuf p) {
    static char buf[136];
    static unsigned char cdb_modsen11[6] = {0x1a, 0x08, 0x11, 0x00,136, 0x00};
    static unsigned char cdb_modsel[6] = {0x15, 0x10, 0x00, 0x00,136, 0x00};
    int res, i;

    res = ftt_do_scsi_command(d,"Get Partition table", cdb_modsen11, 6, buf, 136, 10, 0);
    if (res < 0) return res;

    /* set number of partitions */
    buf[4+3] = p->n_parts;

    /* set to write initiator defined partitions, in MB */
    buf[4+4] = 0x20 | 0x10;

    /* fill in partition sizes... */
    for( i = 0 ; i <= p->n_parts; i++ ) {
        buf[4+8 + 2*i + 0] = (p->partsizes[i] & 0xff00) >> 8;
        buf[4+8 + 2*i + 1] = p->partsizes[i] & 0x00ff;
    }
    res = ftt_do_scsi_command(d,"Put Partition table", cdb_modsel, 6, buf, 136, 1000, 1);
    return res;
}

int
ftt_cur_part(ftt_descriptor d) {
    int res;
    static unsigned char buf[20];
    static unsigned char cdb_read_position[]= {0x34, 0x00, 0x00, 0x00, 0x00,
					    0x00, 0x00, 0x00, 0x00, 0x00};

    res = ftt_do_scsi_command(d,"Read Position", cdb_read_position, 10, 
				  buf, 20, 10, 0);
	
    if (res < 0) {
        return -1;
    } else {
        return buf[1];
    }
}

int		
ftt_skip_part(ftt_descriptor d,int nparts) {
    int cur;
    cur = ftt_cur_part(d);
    cur += nparts;
    ftt_scsi_locate(d,0,cur);
}
