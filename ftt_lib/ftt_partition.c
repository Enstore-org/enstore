static char rcsid[] = "@(#)$Id$";
#include <ftt_private.h>

typedef struct { 
	int n_parts; 
	int max_parts; 
	int partsizes[64]
} *ftt_partbuf;

ftt_partbuf 	
ftt_alloc_parts() {
     ftt_partbuf res;
     res = malloc(sizeof(*res));
     if (res != 0) {
            memset(res, sizeof(*res)),0);
     }
     return res;
}

void 		
ftt_free_parts(ftt_partbuf p) {
   free(p);
}

int 		
ftt_extract_nparts(partbuf p) {
   return p->nparts;
}

int 		
ftt_extract_maxparts(partbuf) {
   return p->maxparts;
}

long 		
ftt_extract_part_size(partbuf p,int n) {
    if ( n > p->nparts || n < 0) {
        ftt_eprintf("not that many partitions in buffer");
	ftt_errno = FTT_EFAULT;
        return -1;
    }
    return p->partsizes[n];
}

int 		
ftt_set_nparts(partbuf p,int n) {
    if ( n < p->maxparts) {
	p->nparts = n;
        return 0;
    } else {
        return -1;
    }
}

int 		
ftt_set_part_size(partbuf p,int n,long sz) {
    if ( n > p->nparts || n < 0) {
        ftt_eprintf("not that many partitions in buffer");
	ftt_errno = FTT_EFAULT;
        return -1;
    }
    p->partsizes[n] = sz;
    return 0;
}

int		
ftt_get_partitions(ftt_descriptor d,partbuf p) {
    static char buf[136];
    static unsigned char cdb_modsen11[6] = {0x1a, 0x00, 0x11, 0x00,136, 0x00};

    res = ftt_do_scsi_command(d,"Get Partition table", cdb_modsen11, 6, buf, 136, 10, 0);
    if (res < 0) return res;
    p->nparts = buf[4+3];
    p->maxparts = buf[4+2];
    for( i = 0 ; i < p->nparts; i++ ) {
        p->partsizes[i] = pack(0,0,buf[4+8+2*i],buf[4+8+2*i+1]);
    }
    return 0;
}

int		
ftt_write_partitions(ftt_descriptor,partbuf) {
    static char buf[136];
    static unsigned char cdb_modsen11[6] = {0x1a, 0x00, 0x11, 0x00,136, 0x00};
    static unsigned char cdb_modsel11[6] = {0x15, 0x11, 0x00, 0x00,136, 0x00};

    res = ftt_do_scsi_command(d,"Get Partition table", cdb_modsen11, 6, buf, 136, 10, 0);
    if (res < 0) return res;
    buf[4+3] = p->nparts;
    for( i = 0 ; i < p->nparts; i++ ) {
        buf[4+8 + 2*i + 0] = (p->partsizes[i] & 0xff00) >> 8;
        buf[4+8 + 2*i + 1] = p->partsizes[i] & 0x00ff
    }
    res = ftt_do_scsi_command(d,"Get Partition table", cdb_modsel11, 6, buf, 136, 10, 1);
    return res;
}

int
ftt_cur_part(ftt_descriptor d) {
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
